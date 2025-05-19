import logging
from pathlib import Path

from imap_db.model import File
from imap_mag import __version__
from imap_mag.db import Database, IDatabase
from imap_mag.io.IFileMetadataProvider import IFileMetadataProvider
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
        # Check database for existing files with same name and path
        preliminary_destination_file: Path = self.assemble_full_path(
            Path(""), metadata_provider
        )
        skip_database_insertion = False

        database_files: list[File] = self.__database.get_files(
            name=preliminary_destination_file.name
        )
        database_files = [
            file
            for file in database_files
            if preliminary_destination_file.parent.as_posix() in file.path
        ]

        logger.info(
            f"Found {len(database_files)} existing files with same name in database."
        )

        # If the hash is the same, skip the file
        original_hash: str = generate_hash(original_file)

        for file in database_files:
            if file.hash == original_hash:
                skip_database_insertion = True
                break

        # If the hash is different, increase the version
        if database_files and not skip_database_insertion:
            metadata_provider.version = self.__get_next_available_version(
                preliminary_destination_file, database_files, metadata_provider
            )

        # Add file locally
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

        # Add file to database
        if skip_database_insertion:
            logger.info(
                f"File {destination_file} already exists in database and is the same. Skipping insertion."
            )
        else:
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

    def __get_next_available_version(
        self,
        destination_file: Path,
        database_files: list[File],
        metadata_provider: IFileMetadataProvider,
    ) -> int:
        """Find a viable version for a file."""

        if not metadata_provider.supports_versioning():
            logger.warning(
                f"File {destination_file} already exists and is different. Overwriting."
            )
            return metadata_provider.version

        max_version: int = max(
            [
                file.version
                for file in database_files
                if file.name == destination_file.name
            ]
        )
        logger.info(
            f"File {destination_file} already exists in database and is different. Increasing version to {max_version + 1}."
        )

        return max_version + 1
