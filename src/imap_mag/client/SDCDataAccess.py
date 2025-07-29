"""Interact with SDC APIs to get MAG data via imap-data-access."""

import logging
from pathlib import Path

import imap_data_access
import imap_data_access.io
from pydantic import SecretStr

from imap_mag.client.SDCQueryParameters import SDCQueryParameters

logger = logging.getLogger(__name__)


class SDCUploadError(Exception):
    """Custom exception for upload errors."""


class SDCDataAccess:
    """Class for uploading and downloading MAG data via imap-data-access."""

    def __init__(
        self, auth_code: SecretStr | None, data_dir: Path, sdc_url: str | None = None
    ) -> None:
        """Initialize SDC API client."""

        imap_data_access.config["API_KEY"] = (
            auth_code.get_secret_value() if auth_code else None
        )
        imap_data_access.config["DATA_DIR"] = data_dir
        imap_data_access.config["DATA_ACCESS_URL"] = sdc_url

    def upload(self, filename: str) -> None:
        logger.debug(f"Uploading {filename} to imap-data-access.")

        try:
            imap_data_access.upload(filename)
        except imap_data_access.io.IMAPDataAccessError as e:
            logger.error(f"Upload failed: {e}")
            raise SDCUploadError(f"Failed to upload file {filename}") from e

    def query(
        self,
        query_parameters: SDCQueryParameters,
    ) -> list[dict[str, str]]:
        return imap_data_access.query(
            table=query_parameters.table,
            **query_parameters.to_dict(),
        )

    def get_filename(
        self,
        query_parameters: SDCQueryParameters,
    ) -> list[dict[str, str]] | None:
        file_details: list[dict[str, str]] = self.query(query_parameters)

        file_names: str = ", ".join([value["file_path"] for value in file_details])
        logger.info(f"Found {len(file_details)} matching files:\n{file_names}")

        return file_details

    def download(self, filename: str) -> Path:
        logger.debug(f"Downloading {filename} from imap-data-access.")
        return imap_data_access.download(filename)
