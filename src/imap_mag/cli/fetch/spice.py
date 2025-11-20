"""Program to retrieve SPICE kernel files from SDC."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.client.SDCDataAccess import SDCDataAccess
from imap_mag.config import AppSettings, FetchMode
from imap_mag.io.DatabaseFileOutputManager import IOutputManager
from imap_mag.io.file import SPICEPathHandler
from imap_mag.io.OutputManager import OutputManager
from imap_mag.util.Humaniser import Humaniser
from imap_mag.util.TimeConversion import TimeConversion

logger = logging.getLogger(__name__)


"""
Example SPICE file metadata from SDC API:
GET https://api.imap-mission.com/spice-query?start_ingest_date=20251101&end_ingest_date=20251105
    [{
        "file_name": "ck/imap_2025_302_2025_303_001.ah.bc",
        "file_root": "imap_2025_302_2025_303_.ah.bc",
        "kernel_type": "attitude_history",
        "version": 1,
        "min_date_j2000": 815036897.0909909,
        "max_date_j2000": 815126896.0094784,
        "file_intervals_j2000": [
            [
                815036897.0909909,
                815126896.0094784
            ]
        ],
        "min_date_datetime": "2025-10-29, 19:07:07",
        "max_date_datetime": "2025-10-30, 20:07:06",
        "file_intervals_datetime": [
            [
                "2025-10-29T19:07:07.908503+00:00",
                "2025-10-30T20:07:06.826978+00:00"
            ]
        ],
        "min_date_sclk": "1/0499460830:00000",
        "max_date_sclk": "1/0499550829:00000",
        "file_intervals_sclk": [
            [
                "1/0499460830:00000",
                "1/0499550829:00000"
            ]
        ],
        "sclk_kernel": "/tmp/naif0012.tls",
        "lsk_kernel": "/tmp/imap_sclk_0031.tsc",
        "ingestion_date": "2025-11-01, 08:05:12",
        "timestamp": 1761984312.0
    }...]

"""


def fetch_spice(
    ingest_start_day: datetime | None = None,
    ingest_end_date: datetime | None = None,
    file_name: str | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    kernel_type: str | None = None,
    latest: bool = False,
    fetch_mode: Annotated[
        FetchMode,
        typer.Option(
            case_sensitive=False,
            help="Whether to download only or download and update progress in database",
        ),
    ] = FetchMode.DownloadOnly,
) -> list[tuple[Path, SPICEPathHandler, dict[str, str]]]:
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
        Dictionary of the downloaded file paths, and the key value metadata from the SDC
    """

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_science)
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)
    data_access = SDCDataAccess(
        auth_code=app_settings.fetch_spice.api.auth_code,
        data_dir=work_folder,
        sdc_url=app_settings.fetch_spice.api.url_base,
    )

    # Query SPICE files from SDC
    spice_file_query_results = data_access.spice_query(
        ingest_start_day=ingest_start_day.date() if ingest_start_day else None,
        ingest_end_date=ingest_end_date.date() if ingest_end_date else None,
        file_name=file_name,
        start_time=start_time,
        end_time=end_time,
        kernel_type=kernel_type,
        latest=latest,
    )

    if not spice_file_query_results:
        logger.info("No SPICE files found matching the query criteria")
        return []

    logger.info(f"Found {len(spice_file_query_results)} SPICE files to download")

    downloaded_spice_files_and_meta = download_spice_files_later_than(
        data_access, ingest_start_day, spice_file_query_results
    )

    output_manager: IOutputManager | None = None
    if not app_settings.fetch_spice.publish_to_data_store:
        logger.info("Files not published to data store based on config.")
    else:
        output_manager = OutputManager.CreateByMode(
            app_settings,
            use_database=(fetch_mode == FetchMode.DownloadAndUpdateProgress),
        )

    output_spice: list[tuple[Path, SPICEPathHandler, dict[str, str]]] = []

    for file_path, file_metadata in downloaded_spice_files_and_meta.items():
        handler = SPICEPathHandler.from_filename(file_path)
        if handler is None:
            logger.error(
                f"Downloaded SPICE file {file_path} could not be parsed into SPICEPathHandler. Skipping publish to data store."
            )
            continue

        handler.add_metadata(file_metadata)
        if output_manager is not None:
            # TODO: add metadata to database
            (output_file, output_handler) = output_manager.add_file(file_path, handler)
            output_spice.append((output_file, output_handler, file_metadata))
        else:
            output_spice.append((file_path, handler, file_metadata))

    return output_spice


def download_spice_files_later_than(
    data_access: SDCDataAccess,
    ingest_start_day: datetime | None,
    spice_file_query_results,
) -> dict[Path, dict[str, str]]:
    downloaded: dict[Path, dict[str, str]] = {}

    for file_meta in [
        f for f in spice_file_query_results if f["file_name"] is not None
    ]:
        file_was_ingested = TimeConversion.try_extract_iso_like_datetime(
            file_meta, "ingestion_date"
        )
        if (
            file_was_ingested
            and ingest_start_day
            and file_was_ingested <= ingest_start_day
        ):
            logger.info(
                f"Skipped {file_meta['file_name']} as SDC ingestion_date {file_was_ingested}, before start date {ingest_start_day}. "
            )
            continue

        downloaded_file = data_access.download(file_meta["file_name"])  # type: ignore
        file_size = downloaded_file.stat().st_size
        if file_size > 0:
            logger.info(
                f"Downloaded {Humaniser.format_bytes(file_size)} {downloaded_file}"
            )
            downloaded[downloaded_file] = file_meta
        else:
            logger.warning(
                f"Downloaded file {downloaded_file} is empty and will not be used."
            )

    logger.info(f"{len(downloaded)} SPICE files downloaded")

    return downloaded
