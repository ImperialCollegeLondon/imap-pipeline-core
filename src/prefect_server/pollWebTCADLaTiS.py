import os
from datetime import datetime, timedelta
from typing import Annotated

from prefect import flow
from prefect.runtime import flow_run
from pydantic import Field, SecretStr

from imap_mag.client.WebTCADLaTiS import WebTCADLaTiS
from imap_mag.config.AppSettings import AppSettings
from imap_mag.db import Database, update_database_with_progress
from imap_mag.io.DatastoreFileManager import DatastoreFileManager
from imap_mag.io.file import HKDecodedPathHandler
from imap_mag.util import CONSTANTS, DatetimeProvider
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import get_secret_or_env_var, try_get_prefect_logger

PROGRESS_ITEM_ID = "SC_LOW_PIVOT_PLATFORM_ANGLE"
LOW_PIVOT_PLATFORM_ANGLE_TMID = 58350
SC_HK_DESCRIPTOR = "low-pivot-platform-angle"
DEFAULT_WEBTCAD_LATIS_URL = "https://lasp.colorado.edu/ops/imap/webtcad/latis/dap/"


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters

    start_date: str = (
        parameters["start_date"].strftime("%d-%m-%Y")
        if parameters["start_date"] is not None
        else "last-update"
    )
    end_date = parameters["end_date"] or DatetimeProvider.end_of_today()

    return f"Download-SC-HK-LowPivotAngle-from-{start_date}-to-{end_date.strftime('%d-%m-%Y')}"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POLL_WEBTCAD_LATIS,
    log_prints=True,
    flow_run_name=generate_flow_run_name,
)
async def poll_webtcad_latis_flow(
    start_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "Start date",
                "description": "Start date for the download. Default is the day after the last progress date.",
            }
        ),
    ] = None,
    end_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "End date",
                "description": "End date for the download. Default is today.",
            }
        ),
    ] = None,
    force_redownload: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Force redownload",
                "description": "If 'True', redownload data for all days in the date range even if previously crawled. Requires 'start_date' and 'end_date'.",
            }
        ),
    ] = False,
):
    """Poll low pivot platform angle data from WebTCAD LaTiS API."""

    logger = try_get_prefect_logger(__name__)
    database = Database()
    settings = AppSettings()

    auth_code = await get_secret_or_env_var(
        PREFECT_CONSTANTS.POLL_WEBTCAD_LATIS.WEBPODA_AUTH_CODE_SECRET_NAME,
        CONSTANTS.ENV_VAR_NAMES.WEBPODA_AUTH_CODE,
    )

    flow_start_timestamp = DatetimeProvider.now()

    # Determine whether this is a manual or automated run
    manual_run: bool = start_date is not None and end_date is not None
    use_database: bool = not manual_run or not force_redownload

    # Determine the date range to download
    if manual_run:
        assert start_date is not None and end_date is not None
        download_start = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        download_end = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        download_end = DatetimeProvider.today()
        workflow_progress = database.get_workflow_progress(PROGRESS_ITEM_ID)
        progress_timestamp = workflow_progress.get_progress_timestamp()

        if progress_timestamp is not None:
            # Start from the day after the last progress date
            download_start = progress_timestamp.replace(
                hour=0, minute=0, second=0, microsecond=0
            ) + timedelta(days=1)
        else:
            download_start = DatetimeProvider.beginning_of_imap().replace(
                hour=0, minute=0, second=0, microsecond=0
            )

    if download_start > download_end:
        logger.info(
            f"Download start {download_start.strftime('%Y-%m-%d')} is after end "
            f"{download_end.strftime('%Y-%m-%d')}. Nothing to download."
        )
        if use_database:
            update_database_with_progress(
                progress_item_id=PROGRESS_ITEM_ID,
                database=database,
                checked_timestamp=flow_start_timestamp,
                latest_timestamp=None,
            )
        return

    logger.info(
        f"Downloading SC HK low pivot platform angle from "
        f"{download_start.strftime('%Y-%m-%d')} to {download_end.strftime('%Y-%m-%d')}."
    )

    datastore_manager = DatastoreFileManager.CreateByMode(
        settings, use_database=use_database
    )

    base_url = os.getenv(
        CONSTANTS.ENV_VAR_NAMES.WEBTCAD_LATIS_URL, DEFAULT_WEBTCAD_LATIS_URL
    )

    client = WebTCADLaTiS(
        auth_code=SecretStr(auth_code),
        base_url=base_url,
    )

    latest_data_date: datetime | None = None
    current_date = download_start

    while current_date <= download_end:
        next_date = current_date + timedelta(days=1)

        logger.info(f"Downloading day {current_date.strftime('%Y-%m-%d')}...")

        csv_content = client.download_csv(
            tmid=LOW_PIVOT_PLATFORM_ANGLE_TMID,
            start_date=current_date,
            end_date=next_date,
        )

        # Check if the CSV has actual data (more than just a header line)
        lines = csv_content.strip().splitlines()
        if len(lines) <= 1:
            logger.info(f"No data for {current_date.strftime('%Y-%m-%d')}. Skipping.")
            current_date = next_date
            continue

        # Write CSV to a temporary file in the work folder
        work_folder = settings.setup_work_folder_for_command(settings.fetch_binary)
        temp_csv_path = (
            work_folder / f"sc_hk_low_pivot_{current_date.strftime('%Y%m%d')}.csv"
        )
        temp_csv_path.write_text(csv_content)

        # Save to datastore using HKDecodedPathHandler
        path_handler = HKDecodedPathHandler(
            instrument="sc",
            descriptor=SC_HK_DESCRIPTOR,
            content_date=current_date,
            extension="csv",
        )

        saved_path, _ = datastore_manager.add_file(temp_csv_path, path_handler)
        logger.info(f"Saved {saved_path}")

        latest_data_date = current_date
        current_date = next_date

    # Update workflow progress
    if use_database:
        latest_timestamp = (
            latest_data_date.replace(hour=23, minute=59, second=59)
            if latest_data_date is not None
            else None
        )
        update_database_with_progress(
            progress_item_id=PROGRESS_ITEM_ID,
            database=database,
            checked_timestamp=flow_start_timestamp,
            latest_timestamp=latest_timestamp,
        )
        if latest_data_date is not None:
            logger.info(f"Updated workflow progress to {latest_data_date}.")
        else:
            logger.info("No new data found. Updated last checked timestamp only.")
    else:
        logger.info("Database not updated (force redownload mode).")

    logger.info("---------- Finished ----------")
