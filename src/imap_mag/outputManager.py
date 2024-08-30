import abc
import hashlib
import logging
import shutil
import typing
from datetime import datetime
from pathlib import Path

import typer


def generate_hash(file: Path) -> str:
    return hashlib.md5(file.read_bytes()).hexdigest()


class IFileMetadataProvider(abc.ABC):
    """Interface for metadata providers."""

    version: int = 0

    @abc.abstractmethod
    def get_folder_structure(self) -> str:
        """Retrieve folder structure."""

    @abc.abstractmethod
    def get_file_name(self) -> str:
        """Retireve file name."""


class DatastoreScienceFilepathGenerator(IFileMetadataProvider):
    """Metadata for output files."""

    prefix: str | None = "imap_mag"
    level: str | None = None
    descriptor: str
    date: datetime
    extension: str

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def get_folder_structure(self) -> str:
        if self.date is None:
            logging.error("No 'date' defined. Cannot generate folder structure.")
            raise typer.Abort()

        return self.date.strftime("%Y/%m/%d")

    def get_file_name(self) -> str:
        if any(x is None for x in ["descriptor", "date", "version", "extension"]):
            logging.error(
                "No 'descriptor', 'date', 'version', or 'extension' defined. Cannot generate file name."
            )
            raise typer.Abort()

        descriptor = self.descriptor

        if self.level is not None:
            descriptor = f"{self.level}_{descriptor}"

        if self.prefix is not None:
            descriptor = f"{self.prefix}_{descriptor}"

        return f"{descriptor}_{self.date.strftime('%Y%m%d')}_v{self.version:03}.{self.extension}"


class IOutputManager(abc.ABC):
    """Interface for output managers."""

    @abc.abstractmethod
    def add_file(
        self, original_file: Path, metadata_provider: IFileMetadataProvider
    ) -> tuple[Path, IFileMetadataProvider]:
        """Add file to output location."""

    def add_default_file(
        self, original_file: Path, **metadata: typing.Any
    ) -> tuple[Path, IFileMetadataProvider]:
        return self.add_file(
            original_file, DatastoreScienceFilepathGenerator(**metadata)
        )


class OutputManager(IOutputManager):
    """Manage output files."""

    location: Path

    def __init__(self, location: Path) -> None:
        self.location = location

    def add_file(
        self, original_file: Path, metadata_provider: IFileMetadataProvider
    ) -> tuple[Path, IFileMetadataProvider]:
        """Add file to output location."""

        if not self.location.exists():
            logging.debug(f"Output location does not exist. Creating {self.location}.")
            self.location.mkdir(parents=True, exist_ok=True)

        destination_file: Path = self.__assemble_full_path(metadata_provider)

        if not destination_file.parent.exists():
            logging.debug(
                f"Output folder structure does not exist. Creating {destination_file.parent}."
            )
            destination_file.parent.mkdir(parents=True, exist_ok=True)

        if destination_file.exists():
            if generate_hash(destination_file) == generate_hash(original_file):
                logging.info(f"File {destination_file} already exists and is the same.")
                return (destination_file, metadata_provider)

            metadata_provider.version = self.__get_next_available_version(
                destination_file, metadata_provider
            )
            destination_file = self.__assemble_full_path(metadata_provider)

        logging.info(f"Copying {original_file} to {destination_file.absolute()}.")
        destination = shutil.copy2(original_file, destination_file)
        logging.info(f"Copied to {destination}.")

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

        while destination_file.exists():
            logging.debug(
                f"File {destination_file} already exists and is different. Increasing version to {metadata_provider.version}."
            )
            metadata_provider.version += 1
            destination_file = self.__assemble_full_path(metadata_provider)

        return metadata_provider.version
