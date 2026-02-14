"""Interact with SDC APIs to get MAG data via imap-data-access."""

import logging
from datetime import date, datetime
from pathlib import Path

import imap_data_access
import imap_data_access.io
import requests
from pydantic import SecretStr

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

    def query_sdc_files(
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
        logger.info(
            f"Querying SDC for files with parameters: level={level}, descriptor={descriptor}, "
            f"start_date={start_date}, end_date={end_date}, "
            f"ingestion_start_date={ingestion_start_date}, ingestion_end_date={ingestion_end_date}, "
            f"version={version}, extension={extension}"
        )

        file_details: list[dict[str, str]] = imap_data_access.query(
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

        file_names: str = ", ".join([value["file_path"] for value in file_details])
        logger.info(f"Found {len(file_details)} matching files:\n{file_names}")

        # if we specified ingestion_start_date or end date then ignore any files that are outside that range
        # because the query is only on date and not the full datetime
        if ingestion_start_date or ingestion_end_date:
            # If the end date does not have a time component then assume the end of the day
            if (
                ingestion_end_date
                and ingestion_end_date.hour == 0
                and ingestion_end_date.minute == 0
            ):
                ingestion_end_date = ingestion_end_date.replace(
                    hour=23, minute=59, second=59
                )

            filtered_files: list[dict[str, str]] = []
            for file in file_details:
                ingestion_date = datetime.strptime(
                    file["ingestion_date"], "%Y%m%d %H:%M:%S"
                )
                if ingestion_start_date and ingestion_date <= ingestion_start_date:
                    continue
                if ingestion_end_date and ingestion_date > ingestion_end_date:
                    continue
                filtered_files.append(file)
            logger.info(
                f"After filtering based on ingestion dates, {len(filtered_files)} files remain."
            )
            return filtered_files

        return file_details

    def download(self, filename: str) -> Path:
        logger.debug(f"Downloading {filename} from imap-data-access.")
        return imap_data_access.download(filename)

    def spice_query(
        self,
        ingest_start_day: date | None = None,
        ingest_end_date: date | None = None,
        file_name: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        kernel_type: str | None = None,
        latest: bool = False,
    ):
        """Query SPICE kernels from the SDC API.

        Args:
            ingest_start_day: Start date for ingestion date filter
            ingest_end_date: End date for ingestion date filter (exclusive)
            file_name: Spice kernel file name filter
            start_time: Coverage start time in TDB seconds
            end_time: Coverage end time in TDB seconds
            kernel_type: Spice kernel type filter. Accepted types are:
                leapseconds, planetary_constants, science_frames, imap_frames,
                spacecraft_clock, planetary_ephemeris, ephemeris_reconstructed,
                ephemeris_nominal, ephemeris_predicted, ephemeris_90days,
                ephemeris_long, ephemeris_launch, attitude_history,
                attitude_predict, pointing_attitude
            latest: If True, only return latest version of kernels matching query

        Returns:
            List of dictionaries containing SPICE kernel metadata
        """
        # Build query parameters
        params = []

        if ingest_start_day:
            date_format = "%Y%m%d"
            params.append(f"start_ingest_date={ingest_start_day.strftime(date_format)}")

        if ingest_end_date:
            date_format = "%Y%m%d"
            params.append(f"end_ingest_date={ingest_end_date.strftime(date_format)}")

        if file_name:
            params.append(f"file_name={file_name}")

        if start_time is not None:
            params.append(f"start_time={start_time}")

        if end_time is not None:
            params.append(f"end_time={end_time}")

        if kernel_type:
            params.append(f"type={kernel_type}")

        if latest:
            params.append("latest=True")

        # Construct URL
        query_string = "&".join(params)
        url = f"{self.get_url_base()}/spice-query?{query_string}"

        logger.info("Querying SPICE files with URL: %s", url)

        # Create a request with the provided URL
        request = requests.Request("GET", url).prepare()

        with imap_data_access.io._make_request(request) as response:
            # Decode the JSON response as a list of items
            items = response.json()
            logger.debug("Received JSON: %s", items)

        return items

    def get_url_base(self):
        return imap_data_access.config["DATA_ACCESS_URL"]
