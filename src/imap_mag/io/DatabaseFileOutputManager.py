import logging
from pathlib import Path

from imap_db.model import File
from imap_mag import __version__
from imap_mag.db import Database, IDatabase
from imap_mag.io.IOutputManager import IOutputManager, T
from imap_mag.io.OutputManager import generate_hash

logger = logging.getLogger(__name__)


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
                    date=metadata_provider.content_date,
                    software_version=__version__,
                )
            )
        except Exception as e:
            logger.error(f"Error inserting {destination_file} into database: {e}")
            destination_file.unlink()
            raise e

        return (destination_file, metadata_provider)
