import hashlib
import logging
import shutil
from pathlib import Path

from imap_mag.io.file import IFilePathHandler, SequenceablePathHandler
from imap_mag.io.IDatastoreFileManager import IDatastoreFileManager, T

logger = logging.getLogger(__name__)


def generate_hash(file: Path) -> str:
    return hashlib.md5(file.read_bytes()).hexdigest()


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

        original_hash = generate_hash(original_file)
        skip_file_copy: bool = self.__get_next_available_version(
            path_handler,
            original_hash=original_hash,
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

        logger.info(f"Copying {original_file} to {destination_file.absolute()}.")
        destination = shutil.copy2(original_file, destination_file)
        logger.debug(f"Copied to {destination}.")

        return (destination_file, path_handler)

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

            destination_file: Path = path_handler.get_full_path(self.location)

            return (
                original_hash == generate_hash(destination_file)
                if destination_file.exists()
                else False
            )
        else:
            assert isinstance(path_handler, SequenceablePathHandler)

        destination_file = path_handler.get_full_path(self.location)

        while destination_file.exists():
            if generate_hash(destination_file) == original_hash:
                return True

            logger.debug(
                f"File {destination_file} already exists and is different. Increasing version to {path_handler.get_sequence() + 1}."
            )
            path_handler.increase_sequence()
            updated_file = path_handler.get_full_path(self.location)

            # Make sure file has changed, otherwise this in an infinite loop
            if destination_file == updated_file:
                logger.error(
                    f"File {destination_file} already exists and is different. Cannot increase version."
                )
                raise FileExistsError(
                    f"File {destination_file} already exists and is different. Cannot increase version."
                )

            destination_file = updated_file

        return False
