"""Interact with SDC APIs to get MAG data via imap-data-access."""

import abc
import logging
from datetime import datetime
from pathlib import Path

import imap_data_access
import imap_data_access.io

from imap_mag.util.constants import CONSTANTS

logger = logging.getLogger(__name__)


class SDCUploadError(Exception):
    """Custom exception for upload errors."""


class ISDCDataAccess(abc.ABC):
    """Interface for interacting with imap-data-access."""

    @staticmethod
    @abc.abstractmethod
    def get_file_path(
        level: str,
        descriptor: str,
        start_date: datetime,
        version: str,
    ) -> tuple[Path, Path]:
        """Get file path for data from imap-data-access."""
        pass

    @abc.abstractmethod
    def upload(self, filename: str) -> None:
        """Upload data to imap-data-access."""
        pass

    @abc.abstractmethod
    def query(
        self,
        *,
        level: str | None = None,
        descriptor: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        ingestion_start_date: datetime | None = None,
        ingestion_end_date: datetime | None = None,
        version: str | None = None,
        extension: str | None = None,
    ) -> list[dict[str, str]]:
        """Download data from imap-data-access."""
        pass

    @abc.abstractmethod
    def get_filename(
        self,
        *,
        level: str | None = None,
        descriptor: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        ingestion_start_date: datetime | None = None,
        ingestion_end_date: datetime | None = None,
        version: str | None = None,
        extension: str | None = None,
    ) -> list[dict[str, str]] | None:
        """Wait for file to be available in imap-data-access."""
        pass

    @abc.abstractmethod
    def download(self, filename: str) -> Path:
        """Download data from imap-data-access."""
        pass


class SDCDataAccess(ISDCDataAccess):
    """Class for uploading and downloading MAG data via imap-data-access."""

    def __init__(self, data_dir: Path, sdc_url: str | None = None) -> None:
        """Initialize SDC API client."""

        imap_data_access.config["DATA_DIR"] = data_dir
        imap_data_access.config["DATA_ACCESS_URL"] = sdc_url or CONSTANTS.SDC_URL

    @staticmethod
    def get_file_path(
        level: str,
        descriptor: str,
        start_date: datetime,
        version: str,
    ) -> tuple[Path, Path]:
        science_file = imap_data_access.ScienceFilePath.generate_from_inputs(
            instrument="mag",
            data_level=level,
            descriptor=descriptor,
            start_time=start_date.strftime("%Y%m%d"),
            version=version,
        )

        return (science_file.filename, science_file.construct_path())

    def upload(self, filename: str) -> None:
        logger.debug(f"Uploading {filename} to imap-data-access.")

        try:
            imap_data_access.upload(filename)
        except imap_data_access.io.IMAPDataAccessError as e:
            logger.error(f"Upload failed: {e}")
            raise SDCUploadError(f"Failed to upload file {filename}") from e

    def query(
        self,
        *,
        level: str | None = None,
        descriptor: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        ingestion_start_date: datetime | None = None,
        ingestion_end_date: datetime | None = None,
        version: str | None = None,
        extension: str | None = None,
    ) -> list[dict[str, str]]:
        return imap_data_access.query(
            instrument="mag",
            data_level=level,
            descriptor=descriptor,
            start_date=(start_date.strftime("%Y%m%d") if start_date else None),
            end_date=(end_date.strftime("%Y%m%d") if end_date else None),
            ingestion_start_date=(
                ingestion_start_date.strftime("%Y%m%d")
                if ingestion_start_date
                else None
            ),
            ingestion_end_date=(
                ingestion_end_date.strftime("%Y%m%d") if ingestion_end_date else None
            ),
            version=version,
            extension=extension,
        )

    def get_filename(
        self,
        *,
        level: str | None = None,
        descriptor: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        ingestion_start_date: datetime | None = None,
        ingestion_end_date: datetime | None = None,
        version: str | None = None,
        extension: str | None = None,
    ) -> list[dict[str, str]] | None:
        file_details: list[dict[str, str]] = self.query(
            level=level,
            descriptor=descriptor,
            start_date=start_date,
            end_date=end_date,
            ingestion_start_date=ingestion_start_date,
            ingestion_end_date=ingestion_end_date,
            version=version,
            extension=extension,
        )

        file_names: str = ", ".join([value["file_path"] for value in file_details])
        logger.info(f"Found {len(file_details)} matching files:\n{file_names}")

        return file_details

    def download(self, filename: str) -> Path:
        logger.debug(f"Downloading {filename} from imap-data-access.")
        return imap_data_access.download(filename)
