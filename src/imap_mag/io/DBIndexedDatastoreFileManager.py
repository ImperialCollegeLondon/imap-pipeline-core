import logging
import re
import shutil
from pathlib import Path

from sqlalchemy.sql import text

from imap_db.model import File
from imap_mag.config.AppSettings import AppSettings
from imap_mag.db import Database
from imap_mag.io.DatastoreFileManager import DatastoreFileManager
from imap_mag.io.file import (
    IFilePathHandler,
    SequenceablePathHandler,
    VersionedPathHandler,
)
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
        # Determine the version: reuse an existing one if content is identical,
        # otherwise advance to the next available slot.
        skip_database_insertion: bool = self.__get_next_available_version(
            original_file,
            path_handler,
        )

        # For a new version the handler may need to rewrite the source file
        # (e.g. update a data_filename reference inside a JSON layer).
        # For a reused version the source is unchanged — the inner file manager
        # will verify the existing destination matches by content identity.
        if not skip_database_insertion:
            actual_source = path_handler.prepare_for_version(original_file)
        else:
            actual_source = original_file

        (destination_file, path_handler) = self.__file_manager.add_file(
            actual_source, path_handler
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
                if path_handler.supports_sequencing()
                and isinstance(
                    path_handler,
                    SequenceablePathHandler,
                )
                else (
                    path_handler.version
                    if isinstance(path_handler, VersionedPathHandler)
                    else 0
                )
            )

            try:
                new_file = File.from_file(
                    file=destination_file,
                    version=version,
                    hash=path_handler.get_content_identity(destination_file),
                    content_date=path_handler.get_content_date_for_indexing(),
                    settings=self.__settings,
                )

                base_meta = path_handler.get_metadata()
                if base_meta:
                    new_file.file_meta = {**(base_meta or {})}

                self.__database.insert_file(new_file)
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
        new_db_path = dest_path.absolute()
        if dest_path.is_relative_to(self.__settings.data_store):
            new_db_path = dest_path.relative_to(self.__settings.data_store)

        archived_file = file.archive_to_new_file_path(new_db_path)
        self.__database.insert_file(archived_file)
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
        original_file: Path,
        path_handler: IFilePathHandler,
    ) -> bool:
        """Find a viable version for a file, returning True if the file already exists unchanged."""

        if not path_handler.supports_sequencing():
            logger.debug(
                "Versioning not supported. File may be overwritten if it already exists and is different."
            )
            return False
        else:
            assert isinstance(path_handler, SequenceablePathHandler)

        database_files: list[File] = self.__get_matching_database_files(path_handler)

        # Check whether an existing version has the same content identity
        identity_hash: str = path_handler.get_content_identity(original_file)
        matching_files: list[File] = [
            f for f in database_files if f.hash == identity_hash
        ]

        assert len(matching_files) <= 1, (
            "There should be at most one file with the same content identity in the database."
        )

        if matching_files:
            logger.info(
                f"File with same content as {original_file.name} already exists in database at version {matching_files[0].version}. Reusing."
            )
            path_handler.set_sequence(matching_files[0].version)
            return True

        # Find the next available version slot
        existing_versions: set[int] = set(file.version for file in database_files)

        while path_handler.get_sequence() in existing_versions:
            current_path = path_handler.get_full_path(Path(""))
            logger.debug(
                f"File {current_path} already exists in database and is different. Increasing version to {path_handler.get_sequence() + 1}."
            )
            path_handler.increase_sequence()

        return False
