"""Interact with SDC APIs to get MAG data via imap-data-access."""

import abc
import logging
import typing
from datetime import datetime
from pathlib import Path

import imap_data_access
import imap_data_access.io
import typing_extensions

logger = logging.getLogger(__name__)


class FileOptions(typing.TypedDict):
    """Options for generating file name."""

    level: str | None
    descriptor: str | None
    start_date: datetime | None
    version: str | None


class VersionOptions(typing.TypedDict):
    """Options for determining unique version."""

    level: str | None
    start_date: datetime | None


class QueryOptions(typing.TypedDict):
    """Options for query."""

    level: str | None
    descriptor: str | None
    start_date: datetime | None
    end_date: datetime | None
    version: str | None
    extension: str | None


class ISDCDataAccess(abc.ABC):
    """Interface for interacting with imap-data-access."""

    @staticmethod
    @abc.abstractmethod
    def get_file_path(
        **options: typing_extensions.Unpack[FileOptions],
    ) -> tuple[Path, Path]:
        """Get file path for data from imap-data-access."""
        pass

    @abc.abstractmethod
    def upload(self, file_name: str) -> None:
        """Upload data to imap-data-access."""
        pass

    @abc.abstractmethod
    def query(
        self, **options: typing_extensions.Unpack[QueryOptions]
    ) -> list[dict[str, str]]:
        """Download data from imap-data-access."""
        pass

    @abc.abstractmethod
    def get_filename(
        self, **options: typing_extensions.Unpack[QueryOptions]
    ) -> list[dict[str, str]] | None:
        """Wait for file to be available in imap-data-access."""
        pass

    @abc.abstractmethod
    def download(self, file_name: str) -> Path:
        """Download data from imap-data-access."""
        pass


class SDCDataAccess(ISDCDataAccess):
    """Class for uploading and downloading MAG data via imap-data-access."""

    def __init__(self, data_dir: Path, sdc_url: str | None = None) -> None:
        """Initialize SDC API client."""

        imap_data_access.config["DATA_DIR"] = data_dir
        imap_data_access.config["DATA_ACCESS_URL"] = (
            sdc_url or "https://api.dev.imap-mission.com"
        )

    @staticmethod
    def get_file_path(
        **options: typing_extensions.Unpack[FileOptions],
    ) -> tuple[Path, Path]:
        science_file = imap_data_access.ScienceFilePath.generate_from_inputs(
            instrument="mag",
            data_level=options["level"],
            descriptor=options["descriptor"],
            start_time=options["start_date"].strftime("%Y%m%d"),
            version=options["version"],
        )

        return (science_file.filename, science_file.construct_path())

    def upload(self, file_name: str) -> None:
        logger.debug(f"Uploading {file_name} to imap-data-access.")

        try:
            imap_data_access.upload(file_name)
        except imap_data_access.io.IMAPDataAccessError as e:
            logger.error(f"Upload failed: {e}")
            raise e

    def query(
        self, **options: typing_extensions.Unpack[QueryOptions]
    ) -> list[dict[str, str]]:
        return imap_data_access.query(
            instrument="mag",
            data_level=options["level"],
            descriptor=options["descriptor"],
            start_date=(
                options["start_date"].strftime("%Y%m%d")
                if options["start_date"]
                else None
            ),
            end_date=(
                options["end_date"].strftime("%Y%m%d") if options["end_date"] else None
            ),
            version=options["version"],
            extension=options["extension"],
        )

    def get_filename(
        self, **options: typing_extensions.Unpack[QueryOptions]
    ) -> list[dict[str, str]] | None:
        file_details: list[dict[str, str]] = self.query(**options)

        file_names: str = ", ".join([value["file_path"] for value in file_details])
        logger.info(f"Found {len(file_details)} matching files:\n{file_names}")

        return file_details

    def download(self, file_name: str) -> Path:
        logger.debug(f"Downloading {file_name} from imap-data-access.")
        return imap_data_access.download(file_name)
