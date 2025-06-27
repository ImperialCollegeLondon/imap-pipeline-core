import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appUtils
from imap_mag.api.apiUtils import initialiseLoggingForCommand
from imap_mag.cli.fetchScience import (
    FetchScience,
    SDCMetadataProvider,
)
from imap_mag.client.sdcDataAccess import SDCDataAccess
from imap_mag.config import AppSettings, FetchMode
from imap_mag.util import Level, MAGSensor, ReferenceFrame, ScienceMode

logger = logging.getLogger(__name__)


# E.g., imap-mag fetch science --start-date 2025-05-02 --end-date 2025-05-03
# E.g., imap-mag fetch science --ingestion-date --start-date 2025-05-02 --end-date 2025-05-03
def fetch_science(
    start_date: Annotated[datetime, typer.Option(help="Start date for the download")],
    end_date: Annotated[datetime, typer.Option(help="End date for the download")],
    use_ingestion_date: Annotated[
        bool,
        typer.Option(
            "--ingestion-date",
            help="Use ingestion date into SDC database, rather than science measurement date",
        ),
    ] = False,
    level: Annotated[
        Level, typer.Option(case_sensitive=False, help="Level to download")
    ] = Level.level_2,
    reference_frame: Annotated[
        ReferenceFrame | None,
        typer.Option(
            "--frame",
            case_sensitive=False,
            help="Reference frame to download for L2. Only used if level is L2.",
        ),
    ] = None,
    modes: Annotated[
        list[ScienceMode],
        typer.Option(
            case_sensitive=False,
            help="Science modes to download",
        ),
    ] = [
        "norm",  # type: ignore
        "burst",  # type: ignore
    ],  # for some reason Typer does not like these being enums
    sensors: Annotated[
        list[MAGSensor], typer.Option(case_sensitive=False, help="Sensors to download")
    ] = [
        MAGSensor.IBS,
        MAGSensor.OBS,
    ],
    fetch_mode: Annotated[
        FetchMode,
        typer.Option(
            case_sensitive=False,
            help="Whether to download only or download and update progress in database",
        ),
    ] = FetchMode.DownloadOnly,
    auth_code: Annotated[
        str | None,
        typer.Option(
            envvar="SDC_AUTH_CODE",
            help="IMAP Science Data Centre API Key",
        ),
    ] = None,
) -> dict[Path, SDCMetadataProvider]:
    """Download science data from the SDC."""

    settings_overrides = (
        {"fetch_science": {"api": {"auth_code": auth_code}}} if auth_code else {}
    )

    app_settings = AppSettings(**settings_overrides)  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_science)
    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    if reference_frame is not None and level != Level.level_2:
        logger.warning(
            f"Reference frame {reference_frame.value} is only applicable for L2 data. Ignoring input value."
        )
        reference_frame = None

    data_access = SDCDataAccess(
        data_dir=work_folder,
        sdc_url=app_settings.fetch_science.api.url_base,
    )

    fetch_science = FetchScience(data_access, modes=modes, sensors=sensors)
    downloaded_science: dict[Path, SDCMetadataProvider] = (
        fetch_science.download_latest_science(
            level=level,
            reference_frame=reference_frame,
            start_date=start_date,
            end_date=end_date,
            use_ingestion_date=use_ingestion_date,
        )
    )

    if not downloaded_science:
        logger.info(
            f"No data downloaded for level {level.value} from {start_date} to {end_date}."
        )
    else:
        logger.debug(
            f"Downloaded {len(downloaded_science)} files:\n{', '.join(str(f) for f in downloaded_science.keys())}"
        )

    output_science: dict[Path, SDCMetadataProvider] = dict()

    if app_settings.fetch_science.publish_to_data_store:
        output_manager = appUtils.getOutputManagerByMode(
            app_settings.data_store,
            use_database=(fetch_mode == FetchMode.DownloadAndUpdateProgress),
        )

        for file, metadata_provider in downloaded_science.items():
            (output_file, output_metadata) = output_manager.add_file(
                file, metadata_provider
            )
            output_science[output_file] = output_metadata
    else:
        logger.info("Files not published to data store based on config.")

    return output_science
