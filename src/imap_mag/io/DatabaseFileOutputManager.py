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
        # Check if the version needs to be increased
        original_hash: str = generate_hash(original_file)

        (metadata_provider.version, skip_database_insertion) = (
            self.__get_next_available_version(
                metadata_provider,
                original_hash=original_hash,
            )
        )

        # Add file locally
        (destination_file, metadata_provider) = self.__output_manager.add_file(
            original_file, metadata_provider
        )

        if not (
            destination_file.exists()
            and (generate_hash(destination_file) == original_hash)
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
                        hash=original_hash,
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

    def __get_matching_database_file(
        self, metadata_provider: IFileMetadataProvider
    ) -> File | None:
        """Get all files in the database with the same name and path."""

        database_files: list[File] = self.__database.get_files(
            name=metadata_provider.get_filename()
        )
        database_files = [
            file
            for file in database_files
            if metadata_provider.get_folder_structure() in file.path
        ]

        return database_files[0] if database_files else None

    def __get_next_available_version(
        self,
        metadata_provider: IFileMetadataProvider,
        original_hash: str,
    ) -> tuple[int, bool]:
        """Find a viable version for a file."""

        if not metadata_provider.supports_versioning():
            logger.warning(
                "Versioning not supported. File may be overwritten if it already exists."
            )
            return (metadata_provider.version, False)

        preliminary_destination_file: Path = self.assemble_full_path(
            Path(""), metadata_provider
        )
        database_file: File | None = self.__get_matching_database_file(
            metadata_provider
        )

        while database_file is not None:
            if original_hash == database_file.hash:
                logger.info(
                    f"File {preliminary_destination_file} already exists in database and is the same. Skipping insertion."
                )
                return (metadata_provider.version, True)

            logger.debug(
                f"File {preliminary_destination_file} already exists in database and is different. Increasing version to {metadata_provider.version + 1}."
            )
            metadata_provider.version += 1
            updated_file: Path = self.assemble_full_path(Path(""), metadata_provider)

            # Make sure file has changed, otherwise this in an infinite loop
            if preliminary_destination_file == updated_file:
                logger.error(
                    f"File {preliminary_destination_file} already exists and is different. Cannot increase version."
                )
                raise FileExistsError(
                    f"File {preliminary_destination_file} already exists and is different. Cannot increase version."
                )

            preliminary_destination_file = updated_file
            database_file = self.__get_matching_database_file(metadata_provider)

        return (metadata_provider.version, False)
