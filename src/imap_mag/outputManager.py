import abc
import hashlib
import logging
import shutil
import typing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def generate_hash(file: Path) -> str:
    return hashlib.md5(file.read_bytes()).hexdigest()


@dataclass
class IFileMetadataProvider(abc.ABC):
    """Interface for metadata providers."""

    version: int = 0

    @abc.abstractmethod
    def supports_versioning(self) -> bool:
        """Check if metadata provider supports versioning."""

    @abc.abstractmethod
    def get_folder_structure(self) -> str:
        """Retrieve folder structure."""

    @abc.abstractmethod
    def get_file_name(self) -> str:
        """Retireve file name."""


@dataclass
class StandardSPDFMetadataProvider(IFileMetadataProvider):
    """
    Metadata for standard SPDF files.
    See: https://imap-processing.readthedocs.io/en/latest/development-guide/style-guide/naming-conventions.html#data-product-file-naming-conventions
    """

    prefix: str | None = "imap_mag"
    level: str | None = None
    descriptor: str | None = None
    date: datetime | None = None  # date data belongs to
    extension: str | None = None

    def supports_versioning(self) -> bool:
        return True

    def get_folder_structure(self) -> str:
        if self.date is None:
            logger.error("No 'date' defined. Cannot generate folder structure.")
            raise ValueError("No 'date' defined. Cannot generate folder structure.")

        return self.date.strftime("%Y/%m/%d")

    def get_file_name(self) -> str:
        if (
            self.descriptor is None
            or self.date is None
            or self.version is None
            or self.extension is None
        ):
            logger.error(
                "No 'descriptor', 'date', 'version', or 'extension' defined. Cannot generate file name."
            )
            raise ValueError(
                "No 'descriptor', 'date', 'version', or 'extension' defined. Cannot generate file name."
            )

        descriptor = self.descriptor

        if self.level is not None:
            descriptor = f"{self.level}_{descriptor}"

        if self.prefix is not None:
            descriptor = f"{self.prefix}_{descriptor}"

        return f"{descriptor}_{self.date.strftime('%Y%m%d')}_v{self.version:03}.{self.extension}"


T = typing.TypeVar("T", bound=IFileMetadataProvider)


class IOutputManager(abc.ABC):
    """Interface for output managers."""

    @abc.abstractmethod
    def add_file(self, original_file: Path, metadata_provider: T) -> tuple[Path, T]:
        """Add file to output location."""

    def add_spdf_format_file(
        self, original_file: Path, **metadata: typing.Any
    ) -> tuple[Path, StandardSPDFMetadataProvider]:
        return self.add_file(original_file, StandardSPDFMetadataProvider(**metadata))


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

        destination_file: Path = self.__assemble_full_path(metadata_provider)

        if not destination_file.parent.exists():
            logger.debug(
                f"Output folder structure does not exist. Creating {destination_file.parent}."
            )
            destination_file.parent.mkdir(parents=True, exist_ok=True)

        if destination_file.exists():
            if generate_hash(destination_file) == generate_hash(original_file):
                logger.info(f"File {destination_file} already exists and is the same.")
                return (destination_file, metadata_provider)

            metadata_provider.version = self.__get_next_available_version(
                destination_file, metadata_provider
            )
            destination_file = self.__assemble_full_path(metadata_provider)

        logger.info(f"Copying {original_file} to {destination_file.absolute()}.")
        destination = shutil.copy2(original_file, destination_file)
        logger.info(f"Copied to {destination}.")

        return (destination_file, metadata_provider)

    def __assemble_full_path(self, metadata_provider: IFileMetadataProvider) -> Path:
        """Assemble full path from metadata."""

        return (
            self.location
            / metadata_provider.get_folder_structure()
            / metadata_provider.get_file_name()
        )

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
                f"File {destination_file} already exists and is different. Increasing version to {metadata_provider.version}."
            )
            metadata_provider.version += 1
            updated_file = self.__assemble_full_path(metadata_provider)

            if destination_file == updated_file:
                logger.error(
                    f"File {destination_file} already exists and is different. Cannot increase version."
                )
                raise FileExistsError(
                    f"File {destination_file} already exists and is different. Cannot increase version."
                )

            destination_file = updated_file

        return metadata_provider.version
