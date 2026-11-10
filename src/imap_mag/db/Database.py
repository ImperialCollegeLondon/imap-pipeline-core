import functools
import itertools
import logging
import os
import threading
from datetime import UTC, datetime
from typing import Any, ClassVar, TypeVar

from sqlalchemy import create_engine, event, or_, select
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import Session, sessionmaker

from imap_db.model import Base, File, FileAnalysis, WorkflowProgress
from imap_mag.db.HeliopuddleSync import (
    FileSnapshot,
    HeliopuddleSync,
    HeliopuddleSyncError,
)

logger = logging.getLogger(__name__)

TBase = TypeVar("TBase", bound=Base)


_FILE_CHANGES_KEY = "heliopuddle_file_changes"


class Database:
    """Database manager.

    When the heliopuddle integration is enabled, every committed change to a
    ``File`` row is also applied to heliopuddle (see HeliopuddleSync).

    Args:
        db_url: database URL; defaults to the SQLALCHEMY_URL environment variable.
        heliopuddle_sync: the sync to apply file changes with. None (the default)
            builds it from AppSettings when the first file change is committed,
            False disables it.
    """

    _engine_cache: ClassVar[dict[str, Engine]] = {}
    _engine_lock: ClassVar[threading.Lock] = threading.Lock()
    __active_session: Session | None

    def __init__(
        self,
        db_url=None,
        heliopuddle_sync: HeliopuddleSync | bool | None = None,
    ):
        env_url = self.get_environment_url()
        if db_url is None and env_url is not None:
            db_url = env_url

        if db_url is None or len(db_url) == 0:
            raise ValueError(
                "No database URL provided. Consider setting SQLALCHEMY_URL environment variable."
            )

        url = make_url(db_url)
        url_key = url.render_as_string(hide_password=False)

        with Database._engine_lock:
            if url_key not in Database._engine_cache:
                Database._engine_cache[url_key] = create_engine(url, pool_pre_ping=True)

        self.engine = Database._engine_cache[url_key]
        self.session = sessionmaker(bind=self.engine)

        self.__heliopuddle_sync: HeliopuddleSync | bool | None = heliopuddle_sync
        self.__heliopuddle_lock = threading.Lock()
        event.listen(self.session, "after_flush", _collect_file_changes)
        event.listen(self.session, "after_commit", self.__apply_file_changes)
        event.listen(self.session, "after_rollback", _discard_file_changes)

    @classmethod
    def get_environment_url(cls) -> str | None:
        return os.getenv("SQLALCHEMY_URL")

    def get_heliopuddle_sync(self) -> HeliopuddleSync | None:
        """The heliopuddle sync for this database, or None when the integration is disabled."""
        with self.__heliopuddle_lock:
            if self.__heliopuddle_sync is None:
                from imap_mag.config.AppSettings import AppSettings

                try:
                    settings = AppSettings()  # type: ignore
                except Exception as e:
                    logger.warning(
                        f"Could not load settings to check the heliopuddle integration, so it is off: {e}"
                    )
                    self.__heliopuddle_sync = False
                else:
                    self.__heliopuddle_sync = (
                        HeliopuddleSync.from_settings(settings) or False
                    )

            sync = self.__heliopuddle_sync
            return sync if isinstance(sync, HeliopuddleSync) else None

    def __apply_file_changes(self, session: Session) -> None:
        """Repeat the file changes just committed in heliopuddle."""
        changes: dict[str, FileSnapshot] = session.info.pop(_FILE_CHANGES_KEY, {})
        if not changes:
            return

        sync = self.get_heliopuddle_sync()
        if sync is None:
            return

        sync.apply_changes(changes.values())

    @staticmethod
    def __session_manager(readonly=False, **session_kwargs):
        """Manage session scope for database operations."""

        def outer_wrapper(func):
            @functools.wraps(func)
            def inner_wrapper(self, *args, **kwargs):
                session = self.session(**session_kwargs)
                try:
                    self.__active_session = session
                    value = func(self, *args, **kwargs)

                    if not readonly:
                        session.commit()
                        logger.debug("Database session committed.")

                    return value
                except HeliopuddleSyncError:
                    # The database change is committed; only heliopuddle is behind.
                    raise
                except Exception as e:
                    if not readonly:
                        session.rollback()
                        logger.error(f"Database session rolled back due to error: {e}")
                    raise e
                finally:
                    session.close()
                    self.__active_session = None

            return inner_wrapper

        return outer_wrapper

    def __get_active_session(self) -> Session:
        if self.__active_session is None:
            raise ValueError(
                "No active session. Use decorator @__session_manager to create session."
            )

        return self.__active_session

    def upsert_file(self, file: File) -> None:
        """Insert a file into the database."""
        self.upsert_files([file])

    @__session_manager()
    def upsert_files(self, files: list[File]) -> None:
        session = self.__get_active_session()
        for file in files:
            # check file does not already exist if this is a new file record (id is none)
            if file.id is None or file.id == 0:
                existing_file = (
                    session.query(File)
                    .filter_by(name=file.name, path=file.path)
                    .first()
                )
                if existing_file is not None:
                    if not existing_file.merge_record(file):
                        continue

                    # otherwise we update the existing database record instead of adding a new one
                    file = existing_file

                session.add(file)
            else:
                session.merge(file)

    @__session_manager(expire_on_commit=False)
    def get_files(self, *args, **kwargs) -> list[File]:
        session = self.__get_active_session()
        return session.query(File).filter(*args).filter_by(**kwargs).all()

    @__session_manager(readonly=True, expire_on_commit=False)
    def get_files_by_path(self, path: str, *args, **kwargs) -> list[File]:
        session = self.__get_active_session()
        return (
            session.query(File)
            .filter(File.path.startswith(path))
            .filter(*args)
            .filter_by(**kwargs)
            .order_by(File.last_modified_date)
            .all()
        )

    def get_files_since(
        self, last_modified_date: datetime, how_many: int | None = None
    ) -> list[File]:
        statement = (
            select(File)
            .where(
                File.last_modified_date > last_modified_date,
                File.deletion_date.is_(None),
            )
            .order_by(File.last_modified_date)
        )

        if how_many is not None:
            statement = statement.limit(how_many)

        logger.debug(f"Executing SQL statement: {statement}")

        with self.session() as session:
            return list(session.execute(statement).scalars().all())

    def get_files_deleted_since(
        self, last_modified_date: datetime, how_many: int | None = None
    ) -> list[File]:
        """Get files that have been marked as deleted since the given date."""
        statement = (
            select(File)
            .where(
                File.deletion_date > last_modified_date,
            )
            .order_by(File.deletion_date)
        )

        if how_many is not None:
            statement = statement.limit(how_many)

        logger.debug(f"Executing SQL statement: {statement}")

        with self.session() as session:
            return list(session.execute(statement).scalars().all())

    @__session_manager(expire_on_commit=False)
    def get_workflow_progress(self, item_name: str) -> WorkflowProgress:
        session = self.__get_active_session()
        workflow_progress = (
            session.query(WorkflowProgress).filter_by(item_name=item_name).first()
        )

        if workflow_progress is None:
            workflow_progress = WorkflowProgress(item_name=item_name)

        return workflow_progress

    def get_all_workflow_progress(self) -> list[WorkflowProgress]:
        statement = select(WorkflowProgress).order_by(
            WorkflowProgress.progress_timestamp.desc()
        )

        with self.session() as session:
            return list(session.execute(statement).scalars().all())

    @__session_manager()
    def save(self, model: Base) -> None:
        session = self.__get_active_session()
        session.merge(model)

    def get_all_active_files(self) -> list[File]:
        """Get all files that have not been deleted."""
        statement = select(File).where(File.deletion_date.is_(None))
        with self.session() as session:
            return list(session.execute(statement).scalars().all())

    def get_files_by_ids(self, file_ids: list[int]) -> list[File]:
        """Get files by their IDs, ordered by last modified date."""
        statement = (
            select(File).where(File.id.in_(file_ids)).order_by(File.last_modified_date)
        )
        with self.session() as session:
            return list(session.execute(statement).scalars().all())

    def get_item_by_key(
        self, item_type: type[TBase], key: dict[str, Any]
    ) -> TBase | None:
        """Get the first item of the given model type whose columns equal the key.

        Args:
            item_type: The model class to query.
            key: Column names mapped to the values they must equal, e.g. {"file_id": 7}.
        """
        statement = select(item_type).filter_by(**key)
        with self.session() as session:
            return session.execute(statement).scalars().first()

    def get_file_analysis_by_file_id(self, file_id: int) -> FileAnalysis | None:
        """Get the file index entry for a given file ID."""
        statement = select(FileAnalysis).where(FileAnalysis.file_id == file_id)
        with self.session() as session:
            return session.execute(statement).scalars().first()

    def get_files_by_path_pattern(self, pattern: str) -> list[File]:
        """Get all active files matching a path pattern (SQL LIKE pattern)."""
        statement = select(File).where(
            File.deletion_date.is_(None),
            File.path.like(pattern),
        )
        with self.session() as session:
            return list(session.execute(statement).scalars().all())

    def get_active_files_matching_patterns(self, patterns: list[str]) -> list[File]:
        """Get all active files matching any of the given fnmatch patterns.

        Args:
            patterns: List of fnmatch patterns (e.g., "*hk/mag/l1/*")

        Returns:
            List of matching File objects. Empty if no patterns are given.
        """
        if not patterns:
            return []

        return self.get_active_files_by_path_or_modified_date(patterns=patterns)

    def get_active_files_by_path_or_modified_date(
        self,
        patterns: list[str] | None = None,
        modified_after: datetime | None = None,
        modified_before: datetime | None = None,
    ) -> list[File]:
        """Get active files matching path patterns and/or a modified date range.

        Converts fnmatch patterns (using * and ?) to SQL LIKE patterns (using % and _)
        and queries the database server-side. Each filter that is provided must match.

        Args:
            patterns: Optional fnmatch patterns (e.g., "*hk/mag/l1/*"); a file
                matching any one of them is included.
            modified_after: Only include files modified after this datetime.
            modified_before: Only include files modified at or before this datetime.

        Returns:
            List of matching File objects, ordered by last modified date.
        """
        statement = (
            select(File)
            .where(File.deletion_date.is_(None))
            .order_by(File.last_modified_date)
        )

        if patterns:
            statement = statement.where(
                or_(*[File.path.like(self._fnmatch_to_like(p)) for p in patterns])
            )

        # last_modified_date is stored as a naive UTC datetime
        if modified_after is not None:
            statement = statement.where(
                File.last_modified_date > self._to_naive_utc(modified_after)
            )
        if modified_before is not None:
            statement = statement.where(
                File.last_modified_date <= self._to_naive_utc(modified_before)
            )

        with self.session() as session:
            return list(session.execute(statement).scalars().all())

    @staticmethod
    def _to_naive_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value
        return value.astimezone(UTC).replace(tzinfo=None)

    @staticmethod
    def _fnmatch_to_like(pattern: str) -> str:
        """Convert an fnmatch pattern to a SQL LIKE pattern.

        Converts:
        - * -> % (match any sequence of characters)
        - ? -> _ (match any single character)
        - Escapes existing % and _ characters

        Args:
            pattern: fnmatch pattern

        Returns:
            SQL LIKE pattern
        """
        # Escape existing SQL LIKE special characters first
        result = pattern.replace("%", r"\%").replace("_", r"\_")
        # Convert fnmatch wildcards to SQL LIKE wildcards
        result = result.replace("**", "%").replace("*", "%").replace("?", "_")
        return result


def _collect_file_changes(session: Session, flush_context: Any) -> None:
    """Snapshot every File row this flush inserts, changes or deletes."""
    changes: dict[str, FileSnapshot] = session.info.setdefault(_FILE_CHANGES_KEY, {})
    for obj in itertools.chain(session.new, session.dirty, session.deleted):
        if not isinstance(obj, File):
            continue
        deleted = obj in session.deleted
        if obj in session.new or deleted or session.is_modified(obj):
            snapshot = FileSnapshot.from_file(obj, hard_deleted=deleted)
            changes[snapshot.path] = snapshot


def _discard_file_changes(session: Session) -> None:
    session.info.pop(_FILE_CHANGES_KEY, None)


def update_database_with_progress(
    progress_item_id: str,
    database: Database,
    checked_timestamp: datetime,
    latest_timestamp: datetime | None,
) -> None:
    workflow_progress = database.get_workflow_progress(progress_item_id)

    logger.debug(
        f"Latest progress timestamp for {progress_item_id} is {latest_timestamp}."
    )

    workflow_progress.update_last_checked_timestamp(checked_timestamp)

    if latest_timestamp and (
        (workflow_progress.progress_timestamp is None)
        or (latest_timestamp > workflow_progress.progress_timestamp)
    ):
        workflow_progress.update_progress_timestamp(latest_timestamp)
    else:
        logger.info(
            f"Latest progress timestamp not updated for {progress_item_id} - no new data found"
        )

    database.save(workflow_progress)
