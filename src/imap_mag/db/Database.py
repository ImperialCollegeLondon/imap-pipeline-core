import functools
import logging
import os
from datetime import datetime

from sqlalchemy import create_engine, or_, select
from sqlalchemy.orm import Session, sessionmaker

from imap_db.model import Base, File, WorkflowProgress

logger = logging.getLogger(__name__)


class Database:
    """Database manager."""

    __active_session: Session | None

    def __init__(self, db_url=None):
        env_url = os.getenv("SQLALCHEMY_URL")
        if db_url is None and env_url is not None:
            db_url = env_url

        if db_url is None or len(db_url) == 0:
            raise ValueError(
                "No database URL provided. Consider setting SQLALCHEMY_URL environment variable."
            )

        self.engine = create_engine(db_url, pool_pre_ping=True)
        self.session = sessionmaker(bind=self.engine)

    @staticmethod
    def __session_manager(
        **session_kwargs,
    ):
        """Manage session scope for database operations."""

        def outer_wrapper(func):
            @functools.wraps(func)
            def inner_wrapper(self, *args, **kwargs):
                session = self.session(**session_kwargs)
                try:
                    self.__active_session = session
                    value = func(self, *args, **kwargs)

                    session.commit()
                    logger.debug("Database session committed.")

                    return value
                except Exception as e:
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

    def insert_file(self, file: File) -> None:
        """Insert a file into the database."""
        self.insert_files([file])

    @__session_manager()
    def insert_files(self, files: list[File]) -> None:
        session = self.__get_active_session()
        for file in files:
            # check file does not already exist
            existing_file = (
                session.query(File).filter_by(name=file.name, path=file.path).first()
            )
            if existing_file is not None:
                if existing_file.hash == file.hash:
                    logger.warning(
                        f"File {file.path} already exists in database. Skipping."
                    )
                else:
                    logger.info(
                        f"File {file.path} already exists in database with different hash. Replacing."
                    )
                    existing_file.hash = file.hash
                continue

            session.add(file)

    @__session_manager(expire_on_commit=False)
    def get_files(self, *args, **kwargs) -> list[File]:
        session = self.__get_active_session()
        return session.query(File).filter(*args).filter_by(**kwargs).all()

    @__session_manager(expire_on_commit=False)
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

        return list(self.session().execute(statement).scalars().all())

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

        return list(self.session().execute(statement).scalars().all())

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

        return list(self.session().execute(statement).scalars().all())

    @__session_manager()
    def save(self, model: Base) -> None:
        session = self.__get_active_session()
        session.merge(model)

    def get_all_active_files(self) -> list[File]:
        """Get all files that have not been deleted."""
        statement = select(File).where(File.deletion_date.is_(None))
        return list(self.session().execute(statement).scalars().all())

    def get_files_by_path_pattern(self, pattern: str) -> list[File]:
        """Get all active files matching a path pattern (SQL LIKE pattern)."""
        statement = select(File).where(
            File.deletion_date.is_(None),
            File.path.like(pattern),
        )
        return list(self.session().execute(statement).scalars().all())

    def get_active_files_matching_patterns(self, patterns: list[str]) -> list[File]:
        """Get all active files matching any of the given fnmatch patterns.

        Converts fnmatch patterns (using * and ?) to SQL LIKE patterns (using % and _)
        and queries the database server-side.

        Args:
            patterns: List of fnmatch patterns (e.g., "*hk/mag/l1/*")

        Returns:
            List of matching File objects
        """
        if not patterns:
            return []

        # Convert fnmatch patterns to SQL LIKE patterns
        like_patterns = [self._fnmatch_to_like(p) for p in patterns]

        # Build OR condition for all patterns
        pattern_conditions = [File.path.like(p) for p in like_patterns]

        statement = select(File).where(
            File.deletion_date.is_(None),
            or_(*pattern_conditions),
        )

        return list(self.session().execute(statement).scalars().all())

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
