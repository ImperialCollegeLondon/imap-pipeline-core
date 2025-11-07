"""Program to retrieve SPICE kernel files from SDC."""

import logging
from datetime import datetime
from pathlib import Path

from imap_mag.client.SDCDataAccess import SDCDataAccess

logger = logging.getLogger(__name__)


def fetch_spice(
    data_access: SDCDataAccess,
    ingest_start_day: datetime | None = None,
    ingest_end_date: datetime | None = None,
    file_name: str | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    kernel_type: str | None = None,
    latest: bool = False,
) -> dict[Path, dict[str, str]]:
    """Download SPICE kernel files from the SDC.

    Args:
        data_access: SDCDataAccess instance for API communication
        ingest_start_day: Start date for ingestion date filter
        ingest_end_date: End date for ingestion date filter (exclusive)
        file_name: Spice kernel file name filter
        start_time: Coverage start time in TDB seconds
        end_time: Coverage end time in TDB seconds
        kernel_type: Spice kernel type filter
        latest: If True, only return latest version of kernels matching query

    Returns:
        Dictionary mapping downloaded file paths to their metadata
    """
    downloaded: dict[Path, dict[str, str]] = {}

    # Query SPICE files from SDC
    file_details = data_access.spice_query(
        ingest_start_day=ingest_start_day.date() if ingest_start_day else None,
        ingest_end_date=ingest_end_date.date() if ingest_end_date else None,
        file_name=file_name,
        start_time=start_time,
        end_time=end_time,
        kernel_type=kernel_type,
        latest=latest,
    )

    if not file_details:
        logger.info("No SPICE files found matching the query criteria")
        return downloaded

    logger.info(f"Found {len(file_details)} SPICE files to download")

    # Download each file if it is new
    for file in [f for f in file_details if f["file_name"] is not None]:
        file_was_ingested = parse_ingestion_date(file)
        if (
            file_was_ingested
            and ingest_start_day
            and file_was_ingested <= ingest_start_day
        ):
            logger.info(
                f"Skipped {file['file_name']} as SDC ingestion_date {file_was_ingested}, before start date {ingest_start_day}. "
            )
            continue

        downloaded_file = data_access.download(file["file_name"])  # type: ignore
        file_size = downloaded_file.stat().st_size
        if file_size > 0:
            logger.info(f"Downloaded {file_size}b {downloaded_file}")
            downloaded[downloaded_file] = file
        else:
            logger.warning(
                f"Downloaded file {downloaded_file} is empty and will not be used."
            )

    logger.info(f"{len(downloaded)} SPICE files downloaded")

    return downloaded


def parse_ingestion_date(key_value) -> datetime | None:
    # Handle both "YYYY-MM-DD, HH:MM:SS" and "YYYY-MM-DD HH:MM:SS" formats

    if key_value.get("ingestion_date"):
        ingestion_date_str = key_value["ingestion_date"].replace(", ", " ")
        ingestion_date = datetime.fromisoformat(ingestion_date_str)
        return ingestion_date
    return None
