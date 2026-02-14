import os
from datetime import date, datetime
from pathlib import Path
from typing import Annotated

from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from pydantic import Field

from imap_db.main import create_db, upgrade_db
from imap_mag.cli.fetch.DownloadDateManager import DownloadDateManager
from imap_mag.cli.fetch.spice import fetch_spice
from imap_mag.db import Database, update_database_with_progress
from imap_mag.io.file import SPICEPathHandler
from imap_mag.util import CONSTANTS, DatetimeProvider, Environment, TimeConversion
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import get_secret_or_env_var


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters

    start_date: str = (
        parameters["ingest_start_day"].strftime("%d-%m-%Y")
        if parameters["ingest_start_day"] is not None
        else "no-start"
    )
    end_date: str = (
        parameters["ingest_end_date"].strftime("%d-%m-%Y")
        if parameters["ingest_end_date"] is not None
        else "no-end"
    )
    kernel_type: str = parameters["kernel_type"] or "all"

    return f"Download-SPICE-{kernel_type}-from-{start_date}-to-{end_date}"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POLL_SPICE,
    log_prints=True,
    flow_run_name=generate_flow_run_name,
)
async def poll_spice_flow(
    ingest_start_day: Annotated[
        date | None,
        Field(
            json_schema_extra={
                "title": "Ingestion start date",
                "description": "Start date for ingestion date filter. If not provided, will use last progress date from database.",
            }
        ),
    ] = None,
    ingest_end_date: Annotated[
        date | None,
        Field(
            json_schema_extra={
                "title": "Ingestion end date",
                "description": "End date for ingestion date filter (exclusive). If not provided, will use end of today.",
            }
        ),
    ] = None,
    file_name: Annotated[
        str | None,
        Field(
            json_schema_extra={
                "title": "File name filter",
                "description": "Spice kernel file name filter (optional).",
            }
        ),
    ] = None,
    start_time: Annotated[
        int | None,
        Field(
            json_schema_extra={
                "title": "Coverage start time",
                "description": "Coverage start time in TDB seconds (optional).",
            }
        ),
    ] = None,
    end_time: Annotated[
        int | None,
        Field(
            json_schema_extra={
                "title": "Coverage end time",
                "description": "Coverage end time in TDB seconds (optional).",
            }
        ),
    ] = None,
    kernel_type: Annotated[
        str | None,
        Field(
            json_schema_extra={
                "title": "Kernel type",
                "description": "Spice kernel type filter (optional). Accepted types: leapseconds, planetary_constants, science_frames, imap_frames, spacecraft_clock, planetary_ephemeris, ephemeris_reconstructed, ephemeris_nominal, ephemeris_predicted, ephemeris_90days, ephemeris_long, ephemeris_launch, attitude_history, attitude_predict, pointing_attitude.",
            }
        ),
    ] = None,
    latest: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Latest version only",
                "description": "If True, only return latest version of kernels matching query.",
            }
        ),
    ] = False,
    force_database_update: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Force database update",
                "description": "Whether to force an update of the database with the downloaded SPICE files.",
            }
        ),
    ] = False,
):
    """
    Poll SPICE kernel files from SDC.
    """

    logger = get_run_logger()
    database = Database()
    progress_item_id = "SPICE"
    date_manager = DownloadDateManager(progress_item_id, database)

    auth_code = await get_secret_or_env_var(
        PREFECT_CONSTANTS.POLL_SCIENCE.SDC_AUTH_CODE_SECRET_NAME,
        CONSTANTS.ENV_VAR_NAMES.SDC_AUTH_CODE,
    )

    # Track when we started checking for files
    check_timestamp = DatetimeProvider.now()

    # If this is an automated flow run, use the database to figure out what to download
    fields_all_none_means_automated = (
        ingest_start_day,
        ingest_end_date,
        file_name,
        start_time,
        end_time,
        kernel_type,
    )
    automated_flow_run: bool = all(v is None for v in fields_all_none_means_automated)
    use_database: bool = force_database_update or automated_flow_run

    if ingest_start_day is not None and ingest_end_date is None:
        ingest_end_date = DatetimeProvider.tomorrow().date()

    # Get dates for download
    ingest_date_filter_source = "user input"

    safe_download_dates = date_manager.get_dates_for_download(
        original_start_date=ingest_start_day,
        original_end_date=ingest_end_date,
        validate_with_database=use_database,
    )

    if safe_download_dates is None:
        raise ValueError(f"No dates for download of {progress_item_id}")

    if (
        safe_download_dates[0] != ingest_start_day
        or safe_download_dates[1] != ingest_end_date
    ):
        ingest_date_filter_source = "database progress"

    (ingest_start_day, ingest_end_date) = safe_download_dates

    logger.info(
        f"Downloading {ingest_start_day} to {ingest_end_date} based on {ingest_date_filter_source} for ingestion date filter."
    )

    # Download files from SDC
    with Environment(CONSTANTS.ENV_VAR_NAMES.SDC_AUTH_CODE, auth_code):
        downloaded_spice: list[tuple[Path, SPICEPathHandler, dict[str, str]]] = (
            fetch_spice(
                ingest_start_day=ingest_start_day,
                ingest_end_date=ingest_end_date,
                file_name=file_name,
                start_time=start_time,
                end_time=end_time,
                kernel_type=kernel_type,
                latest=latest,
                use_database=use_database,
            )
        )

    # Update database with latest ingestion date as progress
    if use_database:
        # Find the latest ingestion date from downloaded files
        ingestion_dates: list[datetime] = []
        for _, _, metadata in downloaded_spice:
            ingestion_date = TimeConversion.try_extract_iso_like_datetime(
                metadata, "ingestion_date"
            )
            if ingestion_date:
                ingestion_dates.append(ingestion_date)

        latest_ingestion_date: datetime | None = (
            max(ingestion_dates) if ingestion_dates else None
        )

        update_database_with_progress(
            progress_item_id=progress_item_id,
            database=database,
            checked_timestamp=check_timestamp,
            latest_timestamp=latest_ingestion_date,
        )
    else:
        logger.info(f"Database not updated with progress for {progress_item_id}.")


# Enable quick local dev like `source .env && python -m src.prefect_server.spiceDownloadFlow` and "debug this file" in vscode
if __name__ == "__main__":
    # we are running locally for development?
    if os.environ.get("ENV_NAME", "") == "dev":
        print("Setting up database for dev environment...")
        create_db()
        upgrade_db()

    poll_spice_flow.serve()
