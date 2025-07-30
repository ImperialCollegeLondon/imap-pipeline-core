from datetime import datetime
from pathlib import Path
from typing import Annotated

from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from pydantic import Field

from imap_mag.cli.fetch.science import fetch_science
from imap_mag.config.FetchMode import FetchMode
from imap_mag.db import Database, update_database_with_progress
from imap_mag.io.file import SciencePathHandler
from imap_mag.util import (
    CONSTANTS,
    DatetimeProvider,
    Environment,
    ReferenceFrame,
    ScienceLevel,
    ScienceMode,
    get_dates_for_download,
)
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import get_secret_or_env_var


def convert_ints_to_string(apids: list[int]) -> str:
    return ",".join(str(apid) for apid in apids)


def generate_flow_run_name() -> str:
    parameters = flow_run.parameters

    level: ScienceLevel = parameters["level"]
    modes: list[ScienceMode] = parameters["modes"]
    start_date: str = (
        parameters["start_date"].strftime("%d-%m-%Y")
        if parameters["start_date"] is not None
        else "last-update"
    )
    end_date = parameters["end_date"] or DatetimeProvider.end_of_today()

    return f"Download-{','.join([m.short_name for m in modes])}-{level.value}-from-{start_date}-to-{end_date.strftime('%d-%m-%Y')}"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POLL_SCIENCE,
    log_prints=True,
    flow_run_name=generate_flow_run_name,
)
async def poll_science_flow(
    level: Annotated[
        ScienceLevel,
        Field(
            json_schema_extra={
                "title": "Level to download",
                "description": "Processing level to download. Default is L1c.",
            }
        ),
    ] = ScienceLevel.l1c,
    reference_frame: Annotated[
        ReferenceFrame | None,
        Field(
            json_schema_extra={
                "title": "Reference frame",
                "description": "Reference frame to download for L2. Only used if level is L2.",
            }
        ),
    ] = None,
    modes: Annotated[
        list[ScienceMode],
        Field(
            json_schema_extra={
                "title": "Science modes to download",
                "description": "List of science modes to download. Default is both Normal and Burst.",
            }
        ),
    ] = [ScienceMode.Normal, ScienceMode.Burst],
    start_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "Start date",
                "description": "Start date for the download. Default is the last progress date for the mode (ingestion date).",
            }
        ),
    ] = None,
    end_date: Annotated[
        datetime | None,
        Field(
            json_schema_extra={
                "title": "End date",
                "description": "End date for the download. Default is the end of today (ingestion date).",
            }
        ),
    ] = None,
    force_ingestion_date: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Force input dates to be ingestion dates",
                "description": "If 'True' input dates are the ingestion date. Otherwise, input dates are in S/C clock time. Ignored if 'start_date' and 'end_date' are not provided.",
            }
        ),
    ] = False,
    force_database_update: Annotated[
        bool,
        Field(
            json_schema_extra={
                "title": "Force database update",
                "description": "Whether to force an update of the database with the downloaded science. Ignored if 'start_date' and 'end_date' are not provided.",
            }
        ),
    ] = False,
):
    """
    Poll housekeeping data from WebPODA.
    """

    logger = get_run_logger()
    database = Database()

    auth_code = await get_secret_or_env_var(
        PREFECT_CONSTANTS.POLL_SCIENCE.SDC_AUTH_CODE_SECRET_NAME,
        CONSTANTS.ENV_VAR_NAMES.SDC_AUTH_CODE,
    )

    if force_database_update and not force_ingestion_date:
        logger.warning(
            "Database cannot be updated without forcing ingestion date. Database will not be updated."
        )

    # If this is an automated flow run, use the database to figure out what to download,
    # and use the ingestion date to download data.
    automated_flow_run: bool = (start_date is None) and (end_date is None)
    use_database: bool = (
        force_database_update and force_ingestion_date
    ) or automated_flow_run
    use_ingestion_date: bool = force_ingestion_date or automated_flow_run

    for mode in modes:
        packet_name = mode.packet
        packet_start_timestamp = DatetimeProvider.now()
        database_name = f"{packet_name}_{level.value.upper()}"

        logger.info(f"---------- Downloading Packet {packet_name} ----------")

        packet_dates = get_dates_for_download(
            packet_name=database_name,
            database=database,
            original_start_date=start_date,
            original_end_date=end_date,
            validate_with_database=use_database,
            logger=logger,
        )

        if packet_dates is None:
            continue
        else:
            (packet_start_date, packet_end_date) = packet_dates

        # Download files from SDC
        with Environment(CONSTANTS.ENV_VAR_NAMES.SDC_AUTH_CODE, auth_code):
            downloaded_science: dict[Path, SciencePathHandler] = fetch_science(
                level=level,
                reference_frame=reference_frame,
                modes=[mode],
                start_date=packet_start_date,
                end_date=packet_end_date,
                use_ingestion_date=use_ingestion_date,
                fetch_mode=FetchMode.DownloadAndUpdateProgress,
            )

        if not downloaded_science:
            logger.info(
                f"No data downloaded for packet {packet_name} from {packet_start_date} to {packet_end_date}."
            )

        # Update database with latest ingestion date as progress (for science)
        if use_database:
            ingestion_dates: list[datetime] = [
                metadata.ingestion_date
                for metadata in downloaded_science.values()
                if metadata.ingestion_date
            ]
            latest_ingestion_date: datetime | None = (
                max(ingestion_dates) if ingestion_dates else None
            )

            update_database_with_progress(
                packet_name=database_name,
                database=database,
                checked_timestamp=packet_start_timestamp,
                latest_timestamp=latest_ingestion_date,
                logger=logger,
            )
        else:
            logger.info(f"Database not updated for {packet_name}.")

    logger.info("---------- Finished ----------")
