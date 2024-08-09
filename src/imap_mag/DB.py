import abc
import hashlib
import logging
import os
from pathlib import Path

import typer
from imap_db.model import File
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from imap_mag import __version__
from imap_mag.outputManager import IMetadataProvider, IOutputManager


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
        self.Session = sessionmaker(bind=self.engine)

    def insert_files(self, files: list[File]) -> None:
        session = self.Session()
        try:
            for file in files:
                # check file does not already exist
                existing_file = (
                    session.query(File)
                    .filter_by(name=file.name, path=file.path)
                    .first()
                )
                if existing_file is not None:
                    continue

                session.add(file)

            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()


class DatabaseOutputManager(IOutputManager):
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
        self, original_file: Path, metadata_provider: IMetadataProvider
    ) -> tuple[Path, IMetadataProvider]:
        (destination_file, metadata_provider) = self.__output_manager.add_file(
            original_file, metadata_provider
        )

        file_hash: str = hashlib.md5(original_file.read_bytes()).hexdigest()

        if not (
            destination_file.exists()
            and (hashlib.md5(destination_file.read_bytes()).hexdigest() == file_hash)
        ):
            logging.error(
                f"File {destination_file} does not exist or is not the same as original {original_file}."
            )
            destination_file.unlink(missing_ok=True)
            raise typer.Abort()

        logging.info(f"Inserting {destination_file} into database.")

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
            logging.error(f"Error inserting {destination_file} into database: {e}")
            destination_file.unlink()
            raise e

        return (destination_file, metadata_provider)
