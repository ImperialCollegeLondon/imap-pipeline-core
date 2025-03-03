import abc
import functools
import logging
import os
from datetime import datetime
from pathlib import Path

import typer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from imap_db.model import DownloadProgress, File
from imap_mag import __version__
from imap_mag.outputManager import IFileMetadataProvider, IOutputManager, generate_hash

logger = logging.getLogger(__name__)


class IDatabase(abc.ABC):
    """Interface for database manager."""

    def insert_file(self, file: File) -> None:
        """Insert a file into the database."""
        self.insert_files([file])
        pass

    @abc.abstractmethod
    def insert_files(self, files: list[File]) -> None:
        """Insert a list of files into the database."""
        pass

    @abc.abstractmethod
    def get_download_progress_timestamp(self, item_name: str) -> datetime | None:
        """Get the progress timestamp for an item."""
        pass

    @abc.abstractmethod
    def update_download_progress(
        self,
        item_name: str,
        *,
        progress_timestamp: datetime | None = None,
        last_checked_date: datetime | None = None,
    ) -> None:
        """Update the download progress for an item."""
        pass


class Database(IDatabase):
    """Database manager."""

    def __init__(self, db_url=None):
        env_url = os.getenv("SQLALCHEMY_URL")
        if db_url is None and env_url is not None:
            db_url = env_url

        if db_url is None:
            raise ValueError(
                "No database URL provided. Consider setting SQLALCHEMY_URL environment variable."
            )

        # TODO: Check database is available

        self.engine = create_engine(db_url)
        self.session = sessionmaker(bind=self.engine)

    @staticmethod
    def __session_manager(func):
        """Manage session scope for database operations."""

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            session = self.session()
            try:
                value = func(self, *args, **kwargs)
                session.commit()

                return value
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()

        return wrapper

    @__session_manager
    def insert_files(self, files: list[File]) -> None:
        session = self.session()
        for file in files:
            # check file does not already exist
            existing_file = (
                session.query(File).filter_by(name=file.name, path=file.path).first()
            )
            if existing_file is not None:
                continue

            session.add(file)

    @__session_manager
    def get_download_progress_timestamp(self, item_name: str) -> datetime | None:
        session = self.session()
        download_progress = (
            session.query(DownloadProgress).filter_by(item_name=item_name).first()
        )
        self.update_download_progress(item_name, last_checked_date=datetime.now())

        if download_progress is None:
            return None

        return download_progress.progress_timestamp

    @__session_manager
    def update_download_progress(
        self,
        item_name: str,
        *,
        progress_timestamp: datetime | None = None,
        last_checked_date: datetime | None = None,
    ) -> None:
        session = self.session()
        download_progress = (
            session.query(DownloadProgress).filter_by(item_name=item_name).first()
        )

        if download_progress is None:
            download_progress = DownloadProgress(item_name=item_name)

        if progress_timestamp is not None:
            logger.info(
                f"Updating progress timestamp for {item_name} to {progress_timestamp}."
            )
            download_progress.progress_timestamp = progress_timestamp

        if last_checked_date is not None:
            logger.info(
                f"Updating last checked date for {item_name} to {last_checked_date}."
            )
            download_progress.last_checked_date = last_checked_date

        session.add(download_progress)


class DatabaseFileOutputManager(IOutputManager):
    """Decorator for adding files to database as well as output."""

    __output_manager: IOutputManager
    __database: IDatabase

    def __init__(
        self, output_manager: IOutputManager, database: Database | None = None
    ):
        """Initialize database and output manager."""

        self.__output_manager = output_manager

        if database is None:
            self.__database = Database()
        else:
            self.__database = database

    def add_file(
        self, original_file: Path, metadata_provider: IFileMetadataProvider
    ) -> tuple[Path, IFileMetadataProvider]:
        (destination_file, metadata_provider) = self.__output_manager.add_file(
            original_file, metadata_provider
        )

        file_hash: str = generate_hash(original_file)

        if not (
            destination_file.exists() and (generate_hash(destination_file) == file_hash)
        ):
            logger.error(
                f"File {destination_file} does not exist or is not the same as original {original_file}."
            )
            destination_file.unlink(missing_ok=True)
            raise typer.Abort()

        logger.info(f"Inserting {destination_file} into database.")

        try:
            self.__database.insert_file(
                File(
                    name=destination_file.name,
                    path=destination_file.absolute().as_posix(),
                    version=metadata_provider.version,
                    hash=file_hash,
                    date=metadata_provider.date,
                    software_version=__version__,
                )
            )
        except Exception as e:
            logger.error(f"Error inserting {destination_file} into database: {e}")
            destination_file.unlink()
            raise e

        return (destination_file, metadata_provider)
