"""Keep a heliopuddle project in step with the IMAP ``files`` table.

The IMAP database stays the source of truth. Every committed change to a
``File`` row is repeated in heliopuddle with the client's ``sync_file`` (a new,
restored or changed file, or changed metadata) or ``sync_removed`` (a
soft-deleted file). The client decides what heliopuddle needs (register or
upload, accept or overwrite, metadata), checks the result and retries busy
paths; this module only maps IMAP rows to heliopuddle paths and metadata.

:meth:`HeliopuddleSync.reconcile` runs the same logic over every file in bulk,
which is both the one-off migration and the repair for any change that could
not be applied at the time.
"""

from __future__ import annotations

import logging
import os
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, overload

from heliopuddle.client import FileRecord, HelioPuddleClient, SyncResult
from sqlalchemy import inspect

from imap_db.model import File

if TYPE_CHECKING:
    # Annotations only. Database imports this module, and imap_mag.config's
    # __init__ imports (through the calibration toolkit and imap_mag.io) Database,
    # so run-time imports of either here would be circular.
    from imap_mag.config.AppSettings import AppSettings
    from imap_mag.config.HeliopuddleConfig import HeliopuddleConfig
    from imap_mag.db.Database import Database

logger = logging.getLogger(__name__)

PROGRESS_ITEM_NAME = "heliopuddle-sync"


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
class ImapDbFileSnapshot:
    """The columns of one ``files`` row that heliopuddle mirrors."""

    id: int | None
    path: str
    hash: str | None
    size: int | None
    descriptor: str | None
    content_date: datetime | None
    deletion_date: datetime | None
    file_meta: dict | None
    hard_deleted: bool = False

    @classmethod
    def from_file(cls, file: File, hard_deleted: bool = False) -> ImapDbFileSnapshot:
        """Snapshot a File without triggering any database load.

        Uses only the loaded attribute values, so it is safe inside session
        flush events and on detached objects.
        """
        values = inspect(file).dict
        return cls(
            id=values.get("id"),
            path=values.get("path") or "",
            hash=values.get("hash"),
            size=values.get("size"),
            descriptor=values.get("descriptor"),
            content_date=values.get("content_date"),
            deletion_date=values.get("deletion_date"),
            file_meta=values.get("file_meta"),
            hard_deleted=hard_deleted,
        )

    @property
    def is_deleted(self) -> bool:
        return self.hard_deleted or self.deletion_date is not None

    def heliopuddle_metadata(self) -> dict[str, Any]:
        """The heliopuddle metadata for this file (keys with no value are left out).

        The row's ``file_meta`` keys, plus ``imap_file_id`` and ``descriptor``
        (which win over a ``file_meta`` key of the same name). Only what
        heliopuddle does not already keep: it has its own hash, size, version
        (from the path) and content date.
        """
        metadata: dict[str, Any] = {
            **(self.file_meta or {}),
            "imap_file_id": self.id,
            "descriptor": self.descriptor,
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
    """

    def __init__(
        self,
        config: HeliopuddleConfig,
        data_store: Path,
        client: HelioPuddleClient | None = None,
    ) -> None:
        self.config = config
        self.data_store = data_store
        self._client = client

    @classmethod
    def from_settings(cls, settings: AppSettings) -> HeliopuddleSync | None:
        """Build the sync from AppSettings, or None when the integration is disabled."""
        if not settings.heliopuddle.enabled:
            return None
        return cls(settings.heliopuddle, settings.data_store)

    @property
    def client(self) -> HelioPuddleClient:
        if self._client is None:
            assert self.config.url is not None
            self._client = HelioPuddleClient(
                self.config.url,
                self.config.project,
                timeout=self.config.request_timeout.total_seconds(),
                client_id=self.config.client_id,
                client_secret=self.config.client_secret,
                busy_retries=self.config.busy_retries,
            )
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    # -- applying changes ------------------------------------------------------

    def apply_changes(self, snapshots: Iterable[ImapDbFileSnapshot]) -> SyncSummary:
        """Apply committed IMAP changes to heliopuddle.

        Raises:
            HeliopuddleSyncError: a file could not be synced and ``fail_on_error``
                is set. Every other file is still attempted.
        """
        summary = SyncSummary()
        for snapshot in snapshots:
            self._sync_and_record_outcome(summary, snapshot)

        if summary.failures:
            if self.config.fail_on_error:
                raise HeliopuddleSyncError(summary.failures)
            logger.error(
                f"heliopuddle is out of sync until the next sync: {summary.failures}"
            )
        return summary

    def sync_file(
        self,
        snapshot: ImapDbFileSnapshot,
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

        Raises:
            FileNotFoundError: a live row's file is not in the data store.
            SyncError: heliopuddle did not end up matching the row.
        """
        path = self.store_path(snapshot.path)
        if path is None:
            logger.warning(
                f"Not syncing {snapshot.path} to heliopuddle: it is not inside the data store."
            )
            return SyncAction.SKIPPED

        options: dict[str, Any] = {"dry_run": dry_run}
        if existing is not _MISSING:
            options["current"] = existing

        result: SyncResult
        if snapshot.is_deleted:
            result = self.client.sync_removed(path, **options)
        else:
            result = self.client.sync_file(
                self.data_store / path,
                path,
                self.config.storage_location,
                snapshot.heliopuddle_metadata(),
                in_place=self.config.shared_datastore,
                content_date=_naive_utc(snapshot.content_date),
                **options,
            )

        action = SyncAction(result.action.value)
        if action != SyncAction.UNCHANGED:
            logger.info(
                f"{path}: {action} in heliopuddle{' (dry run)' if dry_run else ''}."
            )
        return action

    def store_path(self, path: str) -> str | None:
        """The heliopuddle path of a ``files`` row path, or None if it is outside the store.

        Rows normally hold a path relative to the data store. A file archived
        outside the store keeps the absolute path it was moved to, which
        heliopuddle cannot hold. Both kinds are normalised (``..`` and ``.``
        removed, without following links) and kept only when they fall inside
        the data store, as a ``/``-separated path relative to it.
        """
        if not path:
            return None
        root = Path(os.path.normpath(self.data_store.absolute()))
        full = Path(os.path.normpath(root / path))  # an absolute path replaces root
        if full == root or not full.is_relative_to(root):
            return None
        return full.relative_to(root).as_posix()

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
        and fixes every difference. Files whose hash, size and metadata already
        match the IMAP row are not re-read. Files that exist only in heliopuddle
        are reported, and removed with ``remove_untracked``.

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
                self._sync_and_record_outcome(
                    summary, ImapDbFileSnapshot.from_file(file), dry_run=dry_run
                )
        else:
            files = database.get_files()
            # find_all reads every page, however many files heliopuddle has.
            live = {
                record.path: record
                for record in self.client.find_all(
                    order="path", storage_location=self.config.storage_location
                )
            }
            logger.info(
                f"Reconciling {len(files)} IMAP file record(s) with {len(live)} live heliopuddle file(s)."
            )
            for file in sorted(files, key=lambda f: f.path):
                snapshot = ImapDbFileSnapshot.from_file(file)
                path = self.store_path(snapshot.path)
                existing = live.pop(path, None) if path is not None else None
                if existing is not None and _already_in_sync(snapshot, existing):
                    summary.add(SyncAction.UNCHANGED)
                    continue
                self._sync_and_record_outcome(summary, snapshot, existing, dry_run)

            summary.untracked = sorted(live)
            for path, record in sorted(live.items()):
                if remove_untracked:
                    try:
                        self.client.sync_removed(path, current=record, dry_run=dry_run)
                        summary.add(SyncAction.REMOVED)
                    except Exception as e:
                        summary.failures[path] = f"{type(e).__name__}: {e}"
                else:
                    logger.warning(
                        f"{path} is in heliopuddle but not in the IMAP database."
                    )

        logger.info(f"heliopuddle sync: {summary.describe()}.")
        if summary.failures:
            raise HeliopuddleSyncError(summary.failures)
        return summary

    # -- helpers -------------------------------------------------------------------

    def _sync_and_record_outcome(
        self,
        summary: SyncSummary,
        snapshot: ImapDbFileSnapshot,
        existing: FileRecord | None = _MISSING,
        dry_run: bool = False,
    ) -> None:
        try:
            summary.add(self.sync_file(snapshot, existing, dry_run))
        except Exception as e:
            logger.error(f"Failed to sync {snapshot.path} to heliopuddle: {e}")
            summary.failures[snapshot.path] = f"{type(e).__name__}: {e}"


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


def _already_in_sync(snapshot: ImapDbFileSnapshot, record: FileRecord) -> bool:
    """True when heliopuddle's record already matches a live IMAP row (no read needed).

    The IMAP hash is the content identity: usually the md5 of the file, the hash
    heliopuddle keeps. Where it is not (a calibration layer's identity is its data
    file's md5) it never matches, and the file is read and compared instead.
    """
    if snapshot.is_deleted:
        return False
    if (record.hash, record.size) != (snapshot.hash, snapshot.size):
        return False
    if record.metadata != snapshot.heliopuddle_metadata():
        return False
    return snapshot.content_date is None or _naive_utc(
        record.content_date
    ) == _naive_utc(snapshot.content_date)


@overload
def _naive_utc(value: datetime) -> datetime: ...
@overload
def _naive_utc(value: None) -> None: ...
def _naive_utc(value: datetime | None) -> datetime | None:
    """A naive UTC time: heliopuddle takes naive content dates as UTC."""
    if value is None or value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)
