"""Keep a heliopuddle project in step with the IMAP ``files`` table.

The IMAP database stays the source of truth. Every committed change to a
``File`` row is repeated in heliopuddle through its client and API:

- a new or restored file is registered (or uploaded when heliopuddle has its own
  storage location),
- a file whose content changed at the same path is overwritten (or its new
  content accepted),
- a change to any other column updates the heliopuddle metadata,
- a soft-deleted file is removed.

:meth:`HeliopuddleSync.reconcile` runs the same logic over every file in bulk,
which is both the one-off migration and the repair for any change that could
not be applied at the time.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, TypeVar

from sqlalchemy import inspect

from imap_db.model import File

if TYPE_CHECKING:
    from heliopuddle.client import FileRecord, HelioPuddleClient

    from imap_mag.config.AppSettings import AppSettings
    from imap_mag.config.HeliopuddleConfig import HeliopuddleConfig
    from imap_mag.db.Database import Database

logger = logging.getLogger(__name__)

T = TypeVar("T")

PROGRESS_ITEM_NAME = "heliopuddle-sync"
HASH_CHUNK_SIZE = 8 * 1024 * 1024


class HeliopuddleSyncError(Exception):
    """heliopuddle could not be brought in line with the IMAP database.

    The IMAP database change is committed; heliopuddle is out of step for the
    listed paths until the next successful sync.

    Attributes:
        failures: path of each failed file mapped to its error message.
    """

    def __init__(self, failures: dict[str, str]) -> None:
        self.failures = failures
        listed = "; ".join(f"{path}: {error}" for path, error in failures.items())
        super().__init__(
            f"heliopuddle is out of sync for {len(failures)} file(s): {listed}"
        )


class SyncAction(StrEnum):
    ADDED = "added"
    UPDATED = "updated"
    METADATA = "metadata"
    REMOVED = "removed"
    UNCHANGED = "unchanged"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class FileSnapshot:
    """The columns of one ``files`` row that heliopuddle mirrors."""

    id: int | None
    name: str
    path: str
    hash: str | None
    size: int | None
    descriptor: str | None
    version: int | None
    version_major: int | None
    content_date: datetime | None
    deletion_date: datetime | None
    software_version: str | None
    file_meta: dict | None
    hard_deleted: bool = False

    @classmethod
    def from_file(cls, file: File, hard_deleted: bool = False) -> FileSnapshot:
        """Snapshot a File without triggering any database load.

        Uses only the loaded attribute values, so it is safe inside session
        flush events and on detached objects.
        """
        values = inspect(file).dict
        return cls(
            id=values.get("id"),
            name=values.get("name") or "",
            path=values.get("path") or "",
            hash=values.get("hash"),
            size=values.get("size"),
            descriptor=values.get("descriptor"),
            version=values.get("version"),
            version_major=values.get("version_major"),
            content_date=values.get("content_date"),
            deletion_date=values.get("deletion_date"),
            software_version=values.get("software_version"),
            file_meta=values.get("file_meta"),
            hard_deleted=hard_deleted,
        )

    @property
    def is_deleted(self) -> bool:
        return self.hard_deleted or self.deletion_date is not None

    def heliopuddle_metadata(self) -> dict[str, Any]:
        """The heliopuddle metadata for this file (keys with no value are left out)."""
        metadata: dict[str, Any] = {
            "imap_file_id": self.id,
            "imap_hash": self.hash,
            "descriptor": self.descriptor,
            "version": self.version,
            "version_major": self.version_major,
            "content_date": (
                self.content_date.isoformat() if self.content_date else None
            ),
            "software_version": self.software_version,
            "file_meta": self.file_meta,
        }
        return {key: value for key, value in metadata.items() if value is not None}


@dataclass
class SyncSummary:
    """The outcome of syncing many files."""

    actions: Counter[SyncAction] = field(default_factory=Counter)
    failures: dict[str, str] = field(default_factory=dict)
    untracked: list[str] = field(default_factory=list)

    def add(self, action: SyncAction) -> None:
        self.actions[action] += 1

    def describe(self) -> str:
        parts = [f"{count} {action}" for action, count in sorted(self.actions.items())]
        if self.untracked:
            parts.append(f"{len(self.untracked)} only in heliopuddle")
        if self.failures:
            parts.append(f"{len(self.failures)} failed")
        return ", ".join(parts) if parts else "nothing to sync"


_MISSING: Any = object()


class HeliopuddleSync:
    """Applies IMAP ``files`` table changes to a heliopuddle project.

    Args:
        config: the heliopuddle settings.
        data_store: the IMAP data store that ``File.path`` is relative to.
        client: optional ready-made client (tests); built from ``config`` otherwise.
        sleep: waits between retries of a busy path (tests replace it).
    """

    def __init__(
        self,
        config: HeliopuddleConfig,
        data_store: Path,
        client: HelioPuddleClient | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self.data_store = data_store
        self._client = client
        self._sleep = sleep

    @classmethod
    def from_settings(cls, settings: AppSettings) -> HeliopuddleSync | None:
        """Build the sync from AppSettings, or None when the integration is disabled."""
        if not settings.heliopuddle.enabled:
            return None
        return cls(settings.heliopuddle, settings.data_store)

    @property
    def client(self) -> HelioPuddleClient:
        if self._client is None:
            try:
                from heliopuddle.client import HelioPuddleClient
            except ImportError as e:  # pragma: no cover - depends on the install
                raise RuntimeError(
                    "heliopuddle integration is enabled but the heliopuddle client is not installed"
                ) from e

            assert self.config.url is not None
            self._client = HelioPuddleClient(
                self.config.url,
                self.config.project,
                user=self.config.user,
                timeout=self.config.request_timeout.total_seconds(),
                token=self.config.token,
                username=self.config.username,
                password=self.config.password,
                client_id=self.config.client_id,
                client_secret=self.config.client_secret,
            )
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    # -- applying changes ------------------------------------------------------

    def apply_changes(self, snapshots: Iterable[FileSnapshot]) -> SyncSummary:
        """Apply committed IMAP changes to heliopuddle.

        Raises:
            HeliopuddleSyncError: a file could not be synced and ``fail_on_error``
                is set. Every other file is still attempted.
        """
        summary = SyncSummary()
        for snapshot in snapshots:
            self._sync_into(summary, snapshot)

        if summary.failures:
            if self.config.fail_on_error:
                raise HeliopuddleSyncError(summary.failures)
            logger.error(
                f"heliopuddle is out of sync until the next sync: {summary.failures}"
            )
        return summary

    def sync_file(
        self,
        snapshot: FileSnapshot,
        existing: FileRecord | None = _MISSING,
        dry_run: bool = False,
    ) -> SyncAction:
        """Make heliopuddle match one IMAP file row.

        Args:
            snapshot: the IMAP row.
            existing: the live heliopuddle record at the same path, None if there
                is none, or omitted to look it up.
            dry_run: decide what to do without changing heliopuddle.

        Returns:
            What was (or, in a dry run, would be) done.
        """
        path = snapshot.path
        if not _is_store_path(path):
            logger.warning(
                f"Not syncing {path} to heliopuddle: it is not inside the data store."
            )
            return SyncAction.SKIPPED

        if existing is _MISSING:
            existing = self._get_live(path)

        if snapshot.is_deleted:
            if existing is None:
                return SyncAction.UNCHANGED
            if not dry_run:
                record = self._retry(lambda: self.client.remove_file(existing.id))
                if record.deleted_at is None:
                    raise RuntimeError(
                        f"heliopuddle did not delete file {existing.id}; copy states: "
                        f"{[(c.storage_location, str(c.state)) for c in record.storage_locations]}"
                    )
            logger.info(f"Removed {path} from heliopuddle.")
            return SyncAction.REMOVED

        local = self.data_store / path
        if not local.is_file():
            raise FileNotFoundError(f"{local} does not exist in the data store")

        metadata = snapshot.heliopuddle_metadata()
        digest, size = _md5_and_size(local)

        if existing is not None and existing.hash == digest:
            if existing.metadata == metadata:
                return SyncAction.UNCHANGED
            if not dry_run:
                self._update_metadata(existing.id, metadata)
            logger.info(f"Updated heliopuddle metadata for {path}.")
            return SyncAction.METADATA

        if dry_run:
            return SyncAction.ADDED if existing is None else SyncAction.UPDATED

        if existing is None:
            record = self._add(snapshot, local, digest, size, metadata)
            action = SyncAction.ADDED
        else:
            record = self._replace(snapshot, existing, local, metadata)
            action = SyncAction.UPDATED

        if record.hash != digest:
            raise RuntimeError(
                f"heliopuddle has hash {record.hash} for {path}, expected {digest}"
            )
        state = self._copy_state(record)
        if state != "available":
            raise RuntimeError(
                f"heliopuddle copy of {path} is {state}, expected available"
            )
        if record.metadata != metadata:
            self._update_metadata(record.id, metadata)

        logger.info(f"{action.capitalize()} {path} in heliopuddle.")
        return action

    # -- bulk migration and reconciliation ---------------------------------------

    def reconcile(
        self,
        database: Database,
        modified_after: datetime | None = None,
        remove_untracked: bool = False,
        dry_run: bool = False,
    ) -> SyncSummary:
        """Bring heliopuddle in line with every file in the IMAP database.

        A full reconcile (no ``modified_after``) is the one-off migration: it
        compares every IMAP row, live or deleted, with the live heliopuddle files
        and fixes every difference. Files whose heliopuddle metadata (which holds
        the IMAP hash) and size already match are not re-read. Files that exist
        only in heliopuddle are reported, and removed with ``remove_untracked``.

        An incremental reconcile only re-syncs IMAP rows modified after
        ``modified_after``.

        Raises:
            HeliopuddleSyncError: any file could not be synced (all are attempted).
        """
        summary = SyncSummary()

        if modified_after is not None:
            since = _naive_utc(modified_after) - self.config.incremental_overlap
            files = database.get_files(File.last_modified_date > since)
            logger.info(
                f"Syncing {len(files)} file(s) modified since {since.isoformat()} to heliopuddle."
            )
            for file in files:
                self._sync_into(summary, FileSnapshot.from_file(file), dry_run=dry_run)
        else:
            files = database.get_files()
            live = {
                record.path: record
                for record in self.client.find_all(
                    order="path",
                    page_size=1000,
                    storage_location=self.config.storage_location,
                )
            }
            logger.info(
                f"Reconciling {len(files)} IMAP file record(s) with {len(live)} live heliopuddle file(s)."
            )
            for file in sorted(files, key=lambda f: f.path):
                snapshot = FileSnapshot.from_file(file)
                existing = live.pop(snapshot.path, None)
                if (
                    existing is not None
                    and not snapshot.is_deleted
                    and existing.size == snapshot.size
                    and existing.metadata == snapshot.heliopuddle_metadata()
                ):
                    summary.add(SyncAction.UNCHANGED)
                    continue
                self._sync_into(summary, snapshot, existing, dry_run)

            summary.untracked = sorted(live)
            for path, record in sorted(live.items()):
                if remove_untracked:
                    try:
                        if not dry_run:
                            self._retry(lambda r=record: self.client.remove_file(r.id))
                        summary.add(SyncAction.REMOVED)
                    except Exception as e:
                        summary.failures[path] = str(e)
                else:
                    logger.warning(
                        f"{path} is in heliopuddle but not in the IMAP database."
                    )

        logger.info(f"heliopuddle sync: {summary.describe()}.")
        if summary.failures:
            raise HeliopuddleSyncError(summary.failures)
        return summary

    # -- helpers -------------------------------------------------------------------

    def _sync_into(
        self,
        summary: SyncSummary,
        snapshot: FileSnapshot,
        existing: FileRecord | None = _MISSING,
        dry_run: bool = False,
    ) -> None:
        try:
            summary.add(self.sync_file(snapshot, existing, dry_run))
        except Exception as e:
            logger.error(f"Failed to sync {snapshot.path} to heliopuddle: {e}")
            summary.failures[snapshot.path] = f"{type(e).__name__}: {e}"

    def _get_live(self, path: str) -> FileRecord | None:
        from heliopuddle.client import NotFoundError

        try:
            return self.client.get_file_by_path(path)
        except NotFoundError:
            return None

    def _add(
        self,
        snapshot: FileSnapshot,
        local: Path,
        digest: str,
        size: int,
        metadata: dict[str, Any],
    ) -> FileRecord:
        from heliopuddle.client import ConflictError

        content_date = _aware(snapshot.content_date)
        try:
            if self.config.shared_datastore:
                return self._retry(
                    lambda: self.client.register_file(
                        snapshot.path,
                        digest,
                        size,
                        storage_location=self.config.storage_location,
                        metadata=metadata,
                        content_date=content_date,
                    )
                )
            return self._retry(
                lambda: self.client.add_file(
                    local,
                    snapshot.path,
                    storage_location=self.config.storage_location,
                    metadata=metadata,
                    content_date=content_date,
                )
            )
        except ConflictError as e:
            # Another writer registered different content meanwhile.
            return self._replace(snapshot, e.file, local, metadata)

    def _replace(
        self,
        snapshot: FileSnapshot,
        existing: FileRecord,
        local: Path,
        metadata: dict[str, Any],
    ) -> FileRecord:
        if self.config.shared_datastore and self._copy_state(existing) == "unknown":
            # heliopuddle has already seen the new content in the shared store.
            accepted = self._retry(
                lambda: self.client.accept_file(
                    existing.id, storage_location=self.config.storage_location
                )
            )
            if accepted.hash == _md5_and_size(local)[0]:
                return accepted
            existing = accepted

        return self._retry(
            lambda: self.client.add_file(
                local,
                snapshot.path,
                storage_location=self.config.storage_location,
                metadata=metadata,
                overwrite=True,
                content_date=_aware(snapshot.content_date),
            )
        )

    def _update_metadata(self, file_id: int, metadata: dict[str, Any]) -> None:
        self._retry(
            lambda: self.client.update_metadata(file_id, metadata, replace=True)
        )

    def _copy_state(self, record: FileRecord) -> str | None:
        copies = record.storage_locations
        if self.config.storage_location is not None:
            copies = [
                c for c in copies if c.storage_location == self.config.storage_location
            ]
        return str(copies[0].state) if copies else None

    def _retry(self, call: Callable[[], T]) -> T:
        from heliopuddle.client import BusyError

        attempt = 0
        while True:
            try:
                return call()
            except BusyError as e:
                if attempt >= self.config.busy_retries:
                    raise
                attempt += 1
                wait = e.retry_after if e.retry_after is not None else 1.0
                logger.info(
                    f"heliopuddle path is busy; retrying in {wait:.0f}s ({attempt}/{self.config.busy_retries})."
                )
                self._sleep(wait)


def run_sync(
    settings: AppSettings,
    database: Database,
    full: bool = False,
    since: datetime | None = None,
    remove_untracked: bool = False,
    dry_run: bool = False,
    now: datetime | None = None,
) -> SyncSummary:
    """Run a full or incremental reconcile and record its progress.

    Without ``full`` or ``since`` the sync is incremental from the start of the
    last successful sync, and full when there has been none (the migration).

    Raises:
        ValueError: the integration is disabled.
        HeliopuddleSyncError: any file could not be synced.
    """
    sync = HeliopuddleSync.from_settings(settings)
    if sync is None:
        raise ValueError(
            "heliopuddle integration is disabled; set heliopuddle.enabled in the config"
        )

    started = _naive_utc(now or datetime.now(UTC))
    progress = database.get_workflow_progress(PROGRESS_ITEM_NAME)
    if not full and since is None:
        since = progress.get_progress_timestamp()

    try:
        summary = sync.reconcile(
            database,
            modified_after=None if full else since,
            remove_untracked=remove_untracked,
            dry_run=dry_run,
        )
    finally:
        sync.close()

    if not dry_run:
        progress.update_last_checked_timestamp(started)
        progress.update_progress_timestamp(started)
        database.save(progress)
    return summary


def _is_store_path(path: str) -> bool:
    pure = PurePosixPath(path)
    return bool(path) and not pure.is_absolute() and ".." not in pure.parts


def _md5_and_size(path: Path) -> tuple[str, int]:
    digest = hashlib.md5(usedforsecurity=False)
    size = 0
    with path.open("rb") as f:
        while chunk := f.read(HASH_CHUNK_SIZE):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _aware(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
