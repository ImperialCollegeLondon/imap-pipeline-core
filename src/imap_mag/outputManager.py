import hashlib
import logging
import shutil
import typing
from datetime import datetime
from pathlib import Path

import typer
import typing_extensions


class OutputMetadata(typing.TypedDict):
    """Metadata for output files."""

    data_level: str | None
    descriptor: str
    date: datetime
    version: int | None
    extension: str


class OutputManager:
    """Manage output files."""

    @staticmethod
    def get_folder_structure(**metadata: OutputMetadata) -> str:
        """Retrieve folder structure from metadata."""

        if "date" not in metadata:
            logging.error(f"Metadata must contain key 'date'. Got: {metadata.keys()}")
            raise typer.Abort()

        return metadata["date"].strftime("%Y/%m/%d")

    @staticmethod
    def get_file_name(**metadata: OutputMetadata) -> str:
        """Retireve file name from metadata."""

        if {"descriptor", "date", "version", "extension"} > metadata.keys():
            logging.error(
                f"Metadata must contain keys 'descriptor', 'date', 'version', 'extension'. Got: {metadata.keys()}"
            )
            raise typer.Abort()

        return f"{metadata['descriptor']}-{metadata['date'].strftime('%Y%m%d')}-v{metadata['version']:03}.{metadata['extension']}"

    """Output location."""
    location: Path
    """Function returning folder structure pattern."""
    folder_structure_provider: typing.Callable[..., str] = get_folder_structure
    """Function returning file name pattern."""
    file_name_provider: typing.Callable[..., str] = get_file_name

    def __init__(
        self,
        location: Path,
        *,
        folder_structure_provider: typing.Callable[..., str] | None = None,
        file_name_provider: typing.Callable[..., str] | None = None,
    ) -> None:
        self.location = location

        if folder_structure_provider is not None:
            self.folder_structure_provider = folder_structure_provider

        if file_name_provider is not None:
            self.file_name_provider = file_name_provider

    def add_file(
        self, original_file: Path, **metadata: typing_extensions.Unpack[OutputMetadata]
    ) -> Path:
        """Add file to output location."""

        if ("version" not in metadata) or (metadata["version"] is None):
            logging.debug("No version provided. Setting to 'v000'.")
            metadata["version"] = 0

        if not self.location.exists():
            logging.debug(f"Output location does not exist. Creating {self.location}.")
            self.location.mkdir(parents=True, exist_ok=True)

        destination_file: Path = self.__assemble_full_path(**metadata)

        if not destination_file.parent.exists():
            logging.debug(
                f"Output folder structure does not exist. Creating {destination_file.parent}."
            )
            destination_file.parent.mkdir(parents=True, exist_ok=True)

        if destination_file.exists():
            if (
                hashlib.md5(destination_file.read_bytes()).hexdigest()
                == hashlib.md5(original_file.read_bytes()).hexdigest()
            ):
                logging.info(f"File {destination_file} already exists and is the same.")
                return destination_file

            metadata["version"] = self.__find_viable_version(
                destination_file, **metadata
            )
            destination_file = self.__assemble_full_path(**metadata)

        logging.info(f"Copying {original_file} to {destination_file.absolute()}.")
        destination = shutil.copy2(original_file, destination_file)
        logging.info(f"Copied to {destination}.")

        return destination_file

    def __assemble_full_path(self, **metadata: OutputMetadata) -> Path:
        """Assemble full path from metadata."""

        return (
            self.location
            / self.folder_structure_provider(**metadata)
            / self.file_name_provider(**metadata)
        )

    def __find_viable_version(
        self, destination_file: Path, **metadata: OutputMetadata
    ) -> int:
        """Find a viable version for a file."""

        while destination_file.exists():
            logging.info(
                f"File {destination_file} already exists and is different. Increasing version to {metadata['version']}."
            )
            metadata["version"] += 1
            destination_file = self.__assemble_full_path(**metadata)

        return metadata["version"]
