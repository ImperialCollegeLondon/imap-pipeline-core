import abc
import functools
import logging
import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from imap_db.model import Base, DownloadProgress, File
from imap_mag import __version__
from imap_mag.outputManager import IOutputManager, T, generate_hash

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
    def get_download_progress(self, item_name: str) -> DownloadProgress:
        """Get the progress timestamp for an item."""
        pass

    @abc.abstractmethod
    def save(self, model: Base) -> None:
        """Save an object to the database."""
        pass


# TODO: the filename and this should match
class Database(IDatabase):
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


# TODO: move this to a separate file
class DatabaseFileOutputManager(IOutputManager):
    """Decorator for adding files to database as well as output."""

    __output_manager: IOutputManager
    __database: IDatabase

    def __init__(
        self, output_manager: IOutputManager, database: IDatabase | None = None
    ):
        """Initialize database and output manager."""

        self.__output_manager = output_manager

        if database is None:
            self.__database = Database()
        else:
            self.__database = database

    def add_file(self, original_file: Path, metadata_provider: T) -> tuple[Path, T]:
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
            raise FileNotFoundError(
                f"File {destination_file} does not exist or is not the same as original {original_file}."
            )

        logger.info(f"Inserting {destination_file} into database.")

        try:
            self.__database.insert_file(
                File(
                    name=destination_file.name,
                    path=destination_file.absolute().as_posix(),
                    version=metadata_provider.version,
                    hash=file_hash,
                    size=destination_file.stat().st_size,
                    date=metadata_provider.date,
                    software_version=__version__,
                )
            )
        except Exception as e:
            logger.error(f"Error inserting {destination_file} into database: {e}")
            destination_file.unlink()
            raise e

        return (destination_file, metadata_provider)
