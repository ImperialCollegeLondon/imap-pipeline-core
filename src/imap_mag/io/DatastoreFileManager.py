import logging
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from imap_mag.db.Database import Database
from imap_mag.io.file import IFilePathHandler, SequenceablePathHandler
from imap_mag.io.IDatastoreFileManager import IDatastoreFileManager, T

if TYPE_CHECKING:
    from imap_mag.config.AppSettings import AppSettings

logger = logging.getLogger(__name__)


class DatastoreFileManager(IDatastoreFileManager):
    """Manage output files."""

    location: Path

    def __init__(self, datastore_path: Path) -> None:
        self.location = datastore_path

    def add_file(self, original_file: Path, path_handler: T) -> tuple[Path, T]:
        """Add file to output location."""

        if not original_file.exists():
            logger.error(f"File {original_file} does not exist.")
            raise FileNotFoundError(f"File {original_file} does not exist.")

        if not self.location.exists():
            logger.debug(f"Output location does not exist. Creating {self.location}.")
            self.location.mkdir(parents=True, exist_ok=True)

        skip_file_copy: bool = self.__get_next_available_version(
            original_file,
            path_handler,
        )
        destination_file: Path = path_handler.get_full_path(self.location)

        if destination_file.exists() and destination_file.samefile(original_file):
            logger.info(
                f"Source and destination files are the same ({original_file}). Skipping update."
            )
            return (destination_file, path_handler)

        elif skip_file_copy:
            logger.info(
                f"File {destination_file} already exists and is the same. Skipping update."
            )
            return (destination_file, path_handler)

        elif not destination_file.parent.exists():
            logger.debug(
                f"Output folder structure does not exist. Creating {destination_file.parent}."
            )
            destination_file.parent.mkdir(parents=True, exist_ok=True)

        # Allow the handler to rewrite the source (e.g. update version references in JSON).
        source_file_after_reversioning = path_handler.prepare_for_version(original_file)
        try:
            logger.info(f"Copying {original_file} to {destination_file.absolute()}.")
            destination = shutil.copy2(source_file_after_reversioning, destination_file)
            logger.debug(f"Copied to {destination}.")
            self.verify_file_delivered_to_datastore(
                original_file, source_file_after_reversioning, destination_file
            )
        finally:
            if (
                source_file_after_reversioning != original_file
                and source_file_after_reversioning.exists()
            ):
                source_file_after_reversioning.unlink()

        return (destination_file, path_handler)

    def verify_file_delivered_to_datastore(
        self, original_file, source_file_after_reversioning, destination_file
    ):
        if not destination_file.exists():
            raise FileNotFoundError(
                f"File {destination_file} does not exist after copy from {original_file}."
            )

        def generate_hash(file: Path) -> str:
            return IFilePathHandler.default_file_hash(file)

        if generate_hash(destination_file) != generate_hash(
            source_file_after_reversioning
        ):
            logger.error(
                f"File {destination_file} content differs from reversioned {source_file_after_reversioning} (and maybe source {original_file})."
            )
            raise FileNotFoundError(
                f"File {destination_file} does not match source {original_file}."
            )

    def __get_next_available_version(
        self,
        original_file: Path,
        path_handler: IFilePathHandler,
    ) -> bool:
        """Find a viable version for a file."""

        destination_file: Path = path_handler.get_full_path(self.location)

        if not path_handler.supports_sequencing():
            logger.debug(
                "Versioning not supported. File may be overwritten if it already exists and is different."
            )
            if not destination_file.exists():
                return False
            orig_identity = path_handler.get_content_identity(original_file)
            dest_identity = path_handler.get_content_identity(destination_file)
            return orig_identity == dest_identity
        else:
            assert isinstance(path_handler, SequenceablePathHandler)

        while True:
            if destination_file.exists():
                orig_identity = path_handler.get_content_identity(original_file)
                dest_identity = path_handler.get_content_identity(destination_file)
                if orig_identity == dest_identity:
                    return True

                logger.debug(
                    f"File {destination_file} already exists and is different. Increasing version to {path_handler.get_sequence() + 1}."
                )
            elif path_handler.is_version_blocked_by_sibling(
                path_handler.get_sequence(), self.location, original_file
            ):
                logger.debug(
                    f"Version {path_handler.get_sequence()} blocked by sibling conflict for {destination_file}. Increasing version."
                )
            else:
                return False

            path_handler.increase_sequence()
            updated_file = path_handler.get_full_path(self.location)

            # Make sure file has changed, otherwise this is an infinite loop
            if destination_file == updated_file:
                logger.error(
                    f"File {destination_file} already exists and is different. Cannot increase version."
                )
                raise FileExistsError(
                    f"File {destination_file} already exists and is different. Cannot increase version."
                )

            destination_file = updated_file

    @classmethod
    def CreateByMode(
        cls,
        settings: "AppSettings",
        use_database: bool,
        database: "Database | None" = None,
    ) -> IDatastoreFileManager:
        """Retrieve output manager based on destination and mode."""

        manager: IDatastoreFileManager = DatastoreFileManager(settings.data_store)

        if use_database:
            from imap_mag.io.DBIndexedDatastoreFileManager import (
                DBIndexedDatastoreFileManager,
            )

            return DBIndexedDatastoreFileManager(
                manager, settings=settings, database=database
            )
        else:
            return manager
