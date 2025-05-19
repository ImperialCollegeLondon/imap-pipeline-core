import hashlib
import logging
import shutil
from pathlib import Path

from imap_mag.io.IFileMetadataProvider import IFileMetadataProvider
from imap_mag.io.IOutputManager import IOutputManager, T

logger = logging.getLogger(__name__)


def generate_hash(file: Path) -> str:
    return hashlib.md5(file.read_bytes()).hexdigest()


class OutputManager(IOutputManager):
    """Manage output files."""

    location: Path

    def __init__(self, location: Path) -> None:
        self.location = location

    def add_file(self, original_file: Path, metadata_provider: T) -> tuple[Path, T]:
        """Add file to output location."""

        if not self.location.exists():
            logger.debug(f"Output location does not exist. Creating {self.location}.")
            self.location.mkdir(parents=True, exist_ok=True)

        destination_file: Path = self.assemble_full_path(
            self.location, metadata_provider
        )

        if not destination_file.parent.exists():
            logger.debug(
                f"Output folder structure does not exist. Creating {destination_file.parent}."
            )
            destination_file.parent.mkdir(parents=True, exist_ok=True)

        if destination_file.exists():
            if generate_hash(destination_file) == generate_hash(original_file):
                logger.info(
                    f"File {destination_file} already exists and is the same. Skipping update."
                )
                return (destination_file, metadata_provider)

            metadata_provider.version = self.__get_next_available_version(
                destination_file, metadata_provider
            )
            destination_file = self.assemble_full_path(self.location, metadata_provider)

        logger.info(f"Copying {original_file} to {destination_file.absolute()}.")
        destination = shutil.copy2(original_file, destination_file)
        logger.info(f"Copied to {destination}.")

        return (destination_file, metadata_provider)

    def __get_next_available_version(
        self, destination_file: Path, metadata_provider: IFileMetadataProvider
    ) -> int:
        """Find a viable version for a file."""

        if not metadata_provider.supports_versioning():
            logger.warning(
                f"File {destination_file} already exists and is different. Overwriting."
            )
            return metadata_provider.version

        while destination_file.exists():
            logger.debug(
                f"File {destination_file} already exists and is different. Increasing version to {metadata_provider.version + 1}."
            )
            metadata_provider.version += 1
            updated_file = self.assemble_full_path(self.location, metadata_provider)

            if destination_file == updated_file:
                logger.error(
                    f"File {destination_file} already exists and is different. Cannot increase version."
                )
                raise FileExistsError(
                    f"File {destination_file} already exists and is different. Cannot increase version."
                )

            destination_file = updated_file

        return metadata_provider.version
