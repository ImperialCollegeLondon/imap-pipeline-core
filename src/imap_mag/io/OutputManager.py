import hashlib
import logging
import shutil
from pathlib import Path

from imap_mag.io.IFilePathHandler import IFilePathHandler
from imap_mag.io.IOutputManager import IOutputManager, T

logger = logging.getLogger(__name__)


def generate_hash(file: Path) -> str:
    return hashlib.md5(file.read_bytes()).hexdigest()


class OutputManager(IOutputManager):
    """Manage output files."""

    location: Path

    def __init__(self, location: Path) -> None:
        self.location = location

    def add_file(self, original_file: Path, path_handler: T) -> tuple[Path, T]:
        """Add file to output location."""

        if not self.location.exists():
            logger.debug(f"Output location does not exist. Creating {self.location}.")
            self.location.mkdir(parents=True, exist_ok=True)

        (path_handler.sequence, skip_file_copy) = self.__get_next_available_version(
            path_handler,
            original_hash=generate_hash(original_file),
        )
        destination_file: Path = self.assemble_full_path(self.location, path_handler)

        if skip_file_copy:
            logger.info(
                f"File {destination_file} already exists and is the same. Skipping update."
            )
            return (destination_file, path_handler)

        if not destination_file.parent.exists():
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
    ) -> tuple[int, bool]:
        """Find a viable version for a file."""

        if not path_handler.supports_sequencing():
            logger.warning(
                "Versioning not supported. File may be overwritten if it already exists."
            )
            return (path_handler.sequence, False)

        destination_file: Path = self.assemble_full_path(self.location, path_handler)

        while destination_file.exists():
            if generate_hash(destination_file) == original_hash:
                return (path_handler.sequence, True)

            logger.debug(
                f"File {destination_file} already exists and is different. Increasing version to {path_handler.sequence + 1}."
            )
            path_handler.sequence += 1
            updated_file = self.assemble_full_path(self.location, path_handler)

            # Make sure file has changed, otherwise this in an infinite loop
            if destination_file == updated_file:
                logger.error(
                    f"File {destination_file} already exists and is different. Cannot increase version."
                )
                raise FileExistsError(
                    f"File {destination_file} already exists and is different. Cannot increase version."
                )

            destination_file = updated_file

        return (path_handler.sequence, False)
