import logging
import re
import shutil
from enum import StrEnum
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


class IndexResult(StrEnum):
    """Result of indexing an existing file into the database."""

    INDEXED = "indexed"
    SKIPPED = "skipped"
    RESTORED = "restored"


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
            file_manager if file_manager else DatastoreFileManager(self.__settings)
        )

        if database is None:
            self.__database = Database()
        else:
            self.__database = database

    def add_file(self, original_file: Path, path_handler: T) -> tuple[Path, T, bool]:
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

        (destination_file, path_handler, overwritten) = self.__file_manager.add_file(
            actual_source, path_handler
        )

        # Add file to database
        if skip_database_insertion and not overwritten:
            logger.info(
                f"File {destination_file} already exists in database with same hash. Skipping database update."
            )
        else:
            logger.info(f"Upserting {destination_file} into database.")

            try:
                new_file = self.__create_file_record(destination_file, path_handler)
                self.__database.upsert_file(new_file)
            except Exception as e:
                logger.error(f"Error inserting {destination_file} into database: {e}")
                destination_file.unlink()
                raise e

        return (destination_file, path_handler, overwritten)

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
        self.__database.upsert_files([archived_file, file])

        # Delete from filesystem if it exists
        if source_path.exists():
            logger.info(f"Deleting file {source_path} from filesystem.")
            source_path.unlink()
        else:
            logger.warning(
                f"File {source_path} does not exist on filesystem. It may have already been deleted."
            )

    def index_existing_file(
        self, file: Path, path_handler: IFilePathHandler
    ) -> IndexResult:
        """Index an already-present datastore file into the database.

        Handles three cases:
        - File is active in the database (skip, return IndexResult.SKIPPED).
        - File is not in the database at all (create record, return IndexResult.INDEXED).
        - File is in the database but marked as deleted (restore record, return IndexResult.RESTORED).

        Args:
            file: Absolute path to the file in the datastore.
            path_handler: Path handler for the file.

        Returns:
            IndexResult.SKIPPED, IndexResult.INDEXED, or IndexResult.RESTORED.
        """
        relative_path = File.get_datastore_relative_path(file, self.__settings)
        existing_files: list[File] = self.__database.get_files_by_path(relative_path)

        if len(existing_files) > 1:
            logger.warning(
                f"Multiple database records found for {relative_path}. Ignoring deleted records"
            )
            existing_files = [f for f in existing_files if f.deletion_date is None]

        if len(existing_files) > 1:
            raise Exception(
                f"Multiple database records found for {relative_path}. This should not happen. Check database integrity."
            )

        if existing_files:
            file_record = existing_files[0]
            new_meta = path_handler.get_metadata() or {}
            file_record.file_meta = {
                **(file_record.file_meta or {}),
                **new_meta,
            }
            if file_record.deletion_date is not None:
                logger.info(f"Restoring deleted database record for {relative_path}.")
                file_record.deletion_date = None
                self.__database.save(file_record)
                return IndexResult.RESTORED
            elif new_meta:
                logger.info(
                    f"Updating metadata for {relative_path} in database with new metadata: {new_meta}"
                )
                self.__database.save(file_record)
                return IndexResult.SKIPPED
            else:
                logger.debug(
                    f"File {relative_path} already indexed in database. Skipping."
                )
                return IndexResult.SKIPPED

        new_file = self.__create_file_record(file, path_handler)
        logger.info(f"Indexing {relative_path} into database.")
        self.__database.upsert_file(new_file)
        return IndexResult.INDEXED

    def delete_file(self, file: File) -> None:
        """
        Delete a file and mark it as deleted in the database.

        Args:
            file: File to delete
            datastore: Path to datastore root
            db: Database instance
            deletion_date: Timestamp to record as deletion date
        """
        file_path = file.get_full_path(self.__settings)

        # Delete from filesystem if it exists
        if file_path.exists():
            logger.debug(f"Deleting file {file_path} from filesystem.")
            file_path.unlink()
        else:
            logger.warning(
                f"File {file_path} does not exist on filesystem. It may have already been deleted."
            )

        if file_path.exists():
            logger.error(f"Failed to delete file {file_path} from filesystem.")
            raise FileExistsError(f"Failed to delete file {file_path} from filesystem.")
        else:
            file.set_deleted()
            self.__database.save(file)
            logger.debug(f"Deleted file {file_path} from filesystem and DB")

    def __create_file_record(self, file: Path, path_handler: IFilePathHandler) -> File:
        """Create a File database record from a file and its path handler."""
        if path_handler.supports_sequencing() and isinstance(
            path_handler, SequenceablePathHandler
        ):
            version: int = path_handler.get_sequence()
        elif isinstance(path_handler, VersionedPathHandler):
            version = path_handler.version
        else:
            version = 0

        version_major: int = getattr(path_handler, "version_major", 0)

        new_file = File.from_file(
            file=file,
            version=version,
            version_major=version_major,
            hash=path_handler.get_content_identity(file),
            content_date=path_handler.get_content_date_for_indexing(),
            settings=self.__settings,
        )

        base_meta = path_handler.get_metadata()
        if base_meta:
            new_file.file_meta = {**(base_meta or {})}

        return new_file

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
            and file.deletion_date is None
        ]

        return database_files

    def __get_next_available_version(
        self,
        original_file: Path,
        path_handler: IFilePathHandler,
    ) -> bool:
        """Find a viable version for a file, returning True if the file already exists unchanged."""

        IDENTICAL_FILE_ALREADY_EXISTS = True
        FILE_IS_NEW = False

        if not path_handler.supports_sequencing():
            logger.debug(
                "Versioning not supported. File may be overwritten if it already exists and is different."
            )
            return FILE_IS_NEW
        else:
            assert isinstance(path_handler, SequenceablePathHandler)

        database_files: list[File] = self.__get_matching_database_files(path_handler)

        if not database_files:
            logger.debug(
                f"No existing files found in database for {original_file.name}. Proceeding to add as new."
            )
            return FILE_IS_NEW

        # Check whether an existing version has the same content identity
        identity_hash: str = path_handler.get_content_identity(original_file)
        matching_files: list[File] = [
            f for f in database_files if f.hash == identity_hash
        ]

        if len(matching_files) > 1:
            # Multiple records share the same content identity. This can happen when
            # the database was populated before the single-identity invariant was
            # enforced, or when two runs with different major versions happened to
            # produce identical CSV data - perhaps the user used override to create a
            # new version with an identical hash.  Pick the record whose version_major matches
            # the handler's current major version, falling back to the highest version
            # overall, so we reuse the most relevant existing file.
            duplicate_count = len(matching_files)
            current_major = getattr(path_handler, "version_major", 0)
            same_major = [f for f in matching_files if f.version_major == current_major]
            matching_files = sorted(
                same_major if same_major else matching_files,
                key=lambda f: (f.version_major, f.version),
                reverse=True,
            )
            logger.info(
                f"Found {duplicate_count} records with identical content identity for "
                f"{original_file.name}. "
                f"Reusing version {matching_files[0].version_major}.{matching_files[0].version}."
            )

        if matching_files:
            if path_handler.get_sequence() != matching_files[0].version:
                if path_handler.allow_overwrite:
                    logger.warning(
                        f"File with same content as {original_file.name} already exists in database "
                        f"at version {matching_files[0].version}. Proceeding to save at downloaded "
                        f"version {path_handler.get_sequence()} as overwrite is allowed."
                    )
                    # Fall through to the allow_overwrite block — save at the downloaded version.
                elif not path_handler.can_change_sequence():
                    logger.warning(
                        f"File with same content as {original_file.name} already exists in database "
                        f"at version {matching_files[0].version}. This should not happen for supposedly unique files! "
                        f"Proceeding to save at downloaded version {path_handler.get_sequence()} as this is a locked science file."
                    )
                    # Fall through to the other options below
                else:
                    logger.info(
                        f"File with same content as {original_file.name} already exists in database at different version {matching_files[0].version}. Reusing that version."
                    )
                    path_handler.set_sequence(matching_files[0].version)
                    return IDENTICAL_FILE_ALREADY_EXISTS
            else:
                logger.info(
                    f"File with same content and version as {original_file.name} already in database. Reusing."
                )
                return IDENTICAL_FILE_ALREADY_EXISTS

        # Version override: keep the forced version (no max+1 walk). Resolve any
        # unique-constraint conflict by soft-deleting active DB records that share
        # the same minor version but a different path — those represent the file
        # being overwritten at the operator-supplied version.
        # in normal operations this should never happen because paths are well
        # defined and a match by version+descriptor would have the same path and
        # so would not be soft deleted, it would just update the existing file record.
        if path_handler.allow_overwrite:
            forced_minor = path_handler.get_sequence()
            new_destination = path_handler.get_full_path(self.__settings.data_store)
            conflicting = [
                f
                for f in database_files
                if f.version == forced_minor
                and f.path
                != File.get_datastore_relative_path(
                    new_destination, self.__settings, warn=False
                )
            ]
            for conflict in conflicting:
                conflict_path = conflict.get_full_path(self.__settings)
                logger.warning(
                    f"Version override: soft-deleting conflicting DB record for "
                    f"{conflict.path} (same minor version {forced_minor})."
                )
                conflict.set_deleted()
                self.__database.upsert_file(conflict)
                if conflict_path.exists():
                    conflict_path.unlink()
            return FILE_IS_NEW

        # Assign max+1 rather than the first available slot so version numbers are
        # monotonically increasing even when earlier versions have been deleted or
        # never existed (e.g. existing versions {2} → next is 3, not 1).
        existing_versions: set[int] = set(file.version for file in database_files)
        next_version = max(existing_versions) + 1

        if path_handler.get_sequence() >= next_version:
            return FILE_IS_NEW

        if (
            path_handler.get_sequence() < next_version
            and path_handler.can_change_sequence()
        ):
            logger.info(
                f"Existing versions {sorted(existing_versions)} found in database. "
                f"Assigning next available version {next_version} (max + 1)."
            )

            path_handler.set_sequence(next_version)
            return FILE_IS_NEW

        raise ValueError(
            f"Cannot proceed with adding file {original_file.name}."
            f"Existing version(s) {sorted(existing_versions)} found in database with "
            "different content which cannot be overwritten without allow_overwrite "
            "option and we are not allowed to re-version this type of file"
        )
