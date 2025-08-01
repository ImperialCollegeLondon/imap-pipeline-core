import logging
import re
from pathlib import Path

from imap_db.model import File
from imap_mag.db import Database
from imap_mag.io.file.SequenceablePathHandler import SequenceablePathHandler
from imap_mag.io.IOutputManager import IOutputManager, T
from imap_mag.io.OutputManager import generate_hash

logger = logging.getLogger(__name__)


class DatabaseFileOutputManager(IOutputManager):
    """Decorator for adding files to database as well as output."""

    __output_manager: IOutputManager
    __database: Database

    def __init__(
        self, output_manager: IOutputManager, database: Database | None = None
    ):
        """Initialize database and output manager."""

        self.__output_manager = output_manager

        if database is None:
            self.__database = Database()
        else:
            self.__database = database

    def add_file(self, original_file: Path, path_handler: T) -> tuple[Path, T]:
        # Check if the version needs to be increased
        original_hash: str = generate_hash(original_file)

        (file_version, skip_database_insertion) = self.__get_next_available_version(
            path_handler,
            original_hash=original_hash,
        )
        path_handler.set_sequence(file_version)

        # Add file locally
        (destination_file, path_handler) = self.__output_manager.add_file(
            original_file, path_handler
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
                    File.from_file(
                        file=destination_file,
                        version=path_handler.get_sequence(),
                        original_hash=original_hash,
                        content_date=path_handler.content_date,
                    )
                )
            except Exception as e:
                logger.error(f"Error inserting {destination_file} into database: {e}")
                destination_file.unlink()
                raise e

        return (destination_file, path_handler)

    def __get_matching_database_files(
        self, path_handler: SequenceablePathHandler
    ) -> list[File]:
        """Get all files in the database with the same name and path."""

        matching_filename: str = path_handler.get_filename()
        matching_filename = re.sub(r"v\d{3}", "v%", matching_filename)

        logger.debug(
            f"Searching for files in database with name matching {matching_filename}."
        )

        database_files: list[File] = self.__database.get_files(
            File.name.like(matching_filename)
        )
        database_files = [
            file
            for file in database_files
            if path_handler.get_folder_structure() in file.path
        ]

        return database_files

    def __get_next_available_version(
        self,
        path_handler: SequenceablePathHandler,
        original_hash: str,
    ) -> tuple[int, bool]:
        """Find a viable version for a file."""

        if not path_handler.supports_sequencing():
            logger.warning(
                "Versioning not supported. File may be overwritten if it already exists."
            )
            return (path_handler.get_sequence(), False)

        database_files: list[File] = self.__get_matching_database_files(path_handler)

        # Find the file whose hash matches the original file
        matching_files: list[File] = [
            f for f in database_files if f.hash == original_hash
        ]
        assert len(matching_files) <= 1, (
            "There should be at most one file with the same hash in the database."
        )

        if matching_files:
            path_handler.set_sequence(matching_files[0].version)
            preliminary_file = self.assemble_full_path(Path(""), path_handler)

            return (matching_files[0].version, True)

        # Find first available version (note that this might not be the sequential next version)
        existing_versions: set[int] = set(file.version for file in database_files)

        while path_handler.get_sequence() in existing_versions:
            preliminary_file = self.assemble_full_path(Path(""), path_handler)
            logger.debug(
                f"File {preliminary_file} already exists in database and is different. Increasing version to {path_handler.get_sequence() + 1}."
            )
            path_handler.increase_sequence()

            updated_file: Path = self.assemble_full_path(Path(""), path_handler)
            preliminary_file = updated_file

        return (path_handler.get_sequence(), False)
