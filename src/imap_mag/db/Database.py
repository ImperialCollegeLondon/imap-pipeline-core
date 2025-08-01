import functools
import logging
import os
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from imap_db.model import Base, DownloadProgress, File

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

        self.engine = create_engine(db_url)
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

                    return value
                except Exception as e:
                    session.rollback()
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
                logger.warning(
                    f"File {file.path} already exists in database. Skipping."
                )
                continue

            session.add(file)

    @__session_manager(expire_on_commit=False)
    def get_files(self, *args, **kwargs) -> list[File]:
        session = self.__get_active_session()
        return session.query(File).filter(*args).filter_by(**kwargs).all()

    @__session_manager(expire_on_commit=False)
    def get_download_progress(self, item_name: str) -> DownloadProgress:
        session = self.__get_active_session()
        download_progress = (
            session.query(DownloadProgress).filter_by(item_name=item_name).first()
        )

        if download_progress is None:
            download_progress = DownloadProgress(item_name=item_name)

        return download_progress

    @__session_manager()
    def save(self, model: Base) -> None:
        session = self.__get_active_session()
        session.merge(model)


def update_database_with_progress(
    packet_name: str,
    database: Database,
    checked_timestamp: datetime,
    latest_timestamp: datetime | None,
    logger: logging.Logger | logging.LoggerAdapter,
) -> None:
    download_progress = database.get_download_progress(packet_name)

    logger.debug(
        f"Latest downloaded timestamp for packet {packet_name} is {latest_timestamp}."
    )

    download_progress.record_checked_download(checked_timestamp)

    if latest_timestamp and (
        (download_progress.progress_timestamp is None)
        or (latest_timestamp > download_progress.progress_timestamp)
    ):
        download_progress.record_successful_download(latest_timestamp)
    else:
        logger.info(f"Database not updated for {packet_name} as no new data available.")

    database.save(download_progress)
