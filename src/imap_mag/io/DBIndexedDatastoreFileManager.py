import logging
import re
import shutil
from pathlib import Path

from sqlalchemy.sql import text

from imap_db.model import File
from imap_mag.config.AppSettings import AppSettings
from imap_mag.db import Database
from imap_mag.io.DatastoreFileManager import DatastoreFileManager, generate_hash
from imap_mag.io.file import IFilePathHandler, SequenceablePathHandler
from imap_mag.io.IDatastoreFileManager import IDatastoreFileManager, T

logger = logging.getLogger(__name__)


class DBIndexedDatastoreFileManager(IDatastoreFileManager):
    """Decorator for adding files to database as well as output."""

    __file_manager: IDatastoreFileManager
    __database: Database
    __settings: AppSettings

    def __init__(
        self,
        file_manager: IDatastoreFileManager | None = None,
        database: Database | None = None,
        settings: AppSettings | None = None,
    ):
        """Initialize database and output manager."""

        self.__settings = settings if settings else AppSettings()  # type: ignore
        self.__file_manager = (
            file_manager
            if file_manager
            else DatastoreFileManager(self.__settings.data_store)
        )

        if database is None:
            self.__database = Database()
        else:
            self.__database = database

    def add_file(self, original_file: Path, path_handler: T) -> tuple[Path, T]:
        # Check if the version needs to be increased
        original_hash: str = generate_hash(original_file)

        skip_database_insertion: bool = self.__get_next_available_version(
            path_handler,
            original_hash=original_hash,
        )

        # Add file locally
        (destination_file, path_handler) = self.__file_manager.add_file(
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

            version: int = (
                path_handler.get_sequence()
                if isinstance(path_handler, SequenceablePathHandler)
                else 0
            )

            try:
                self.__database.insert_file(
                    File.from_file(
                        file=destination_file,
                        version=version,
                        hash=original_hash,
                        content_date=path_handler.get_content_date_for_indexing(),
                        settings=self.__settings,
                    )
                )
            except Exception as e:
                logger.error(f"Error inserting {destination_file} into database: {e}")
                destination_file.unlink()
                raise e

        return (destination_file, path_handler)

    def archive_file(
        self,
        file: File,
        archive_folder: Path,
    ) -> None:
        """
        Move a file to the archive folder.

        1. Copy to archive location
        2. Create new database record for archived file
        3. Mark original file as deleted
        4. Delete original file from filesystem

        Args:
            file: File to archive
            datastore: Path to datastore root
            archive_folder: Path to archive folder
            db: Database instance
            archive_date: Timestamp to record as archive/deletion date
        """
        source_path = self.__settings.data_store / file.path
        dest_path = archive_folder / file.path

        # Create destination directory
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy file to archive
        shutil.copy2(source_path, dest_path)

        # if destination path is in datastore use a relative path, otherwise use absolute path
        if dest_path.is_relative_to(self.__settings.data_store):
            new_db_path = str(dest_path.relative_to(self.__settings.data_store))
        else:
            new_db_path = str(dest_path.absolute())

        archived_file = File(
            name=file.name,
            path=new_db_path,
            version=file.version,
            hash=file.hash,
            size=file.size,
            content_date=file.content_date,
            creation_date=file.creation_date,
            software_version=file.software_version,
        )
        self.__database.insert_file(archived_file)

        # Mark original file as deleted
        file.set_deleted()
        self.__database.save(file)
        # Delete original from filesystem
        source_path.unlink()

    def delete_file(self, file: File) -> None:
        """
        Delete a file and mark it as deleted in the database.

        Args:
            file: File to delete
            datastore: Path to datastore root
            db: Database instance
            deletion_date: Timestamp to record as deletion date
        """
        file_path = (
            self.__settings.data_store / file.path
            if not Path(file.path).is_absolute()
            else Path(file.path)
        )

        # Mark as deleted in database first
        file.set_deleted()
        self.__database.save(file)

        # Delete from filesystem if it exists
        if file_path.exists():
            file_path.unlink()

    def __get_matching_database_files(
        self, path_handler: SequenceablePathHandler
    ) -> list[File]:
        """Get all files in the database with the same name and path."""

        matching_regex: re.Pattern = path_handler.get_unsequenced_pattern()
        matching_string: str = re.sub(
            r"\(\?P<[^>]+>([^)]+)\)", r"\1", matching_regex.pattern
        )

        logger.debug(
            f"Searching for files in database with name matching {matching_string}."
        )

        database_files: list[File] = self.__database.get_files(
            text("name ~ :matcher").bindparams(matcher=matching_string),
        )
        database_files = [
            file
            for file in database_files
            if path_handler.get_folder_structure() in file.path
        ]

        return database_files

    def __get_next_available_version(
        self,
        path_handler: IFilePathHandler,
        original_hash: str,
    ) -> bool:
        """Find a viable version for a file."""

        if not path_handler.supports_sequencing():
            logger.debug(
                "Versioning not supported. File may be overwritten if it already exists and is different."
            )
            return False
        else:
            assert isinstance(path_handler, SequenceablePathHandler)

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
            preliminary_file = path_handler.get_full_path(Path(""))

            return True

        # Find first available version (note that this might not be the sequential next version)
        existing_versions: set[int] = set(file.version for file in database_files)

        while path_handler.get_sequence() in existing_versions:
            preliminary_file = path_handler.get_full_path(Path(""))
            logger.debug(
                f"File {preliminary_file} already exists in database and is different. Increasing version to {path_handler.get_sequence() + 1}."
            )
            path_handler.increase_sequence()

            updated_file: Path = path_handler.get_full_path(Path(""))
            preliminary_file = updated_file

        return False
