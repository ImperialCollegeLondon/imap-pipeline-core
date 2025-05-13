import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appConfig, appUtils
from imap_mag.api.apiUtils import commandInit
from imap_mag.cli.fetchScience import (
    FetchScience,
    SDCMetadataProvider,
)
from imap_mag.client.sdcDataAccess import SDCDataAccess
from imap_mag.util import Level, MAGSensor, ScienceMode

logger = logging.getLogger(__name__)


# E.g., imap-mag fetch science --start-date 2025-05-02 --end-date 2025-05-03
# E.g., imap-mag fetch science --ingestion-date --start-date 2025-05-02 --end-date 2025-05-03
def fetch_science(
    auth_code: Annotated[
        str,
        typer.Option(
            envvar="SDC_AUTH_CODE",
            help="IMAP Science Data Centre API Key",
        ),
    ],
    start_date: Annotated[datetime, typer.Option(help="Start date for the download")],
    end_date: Annotated[datetime, typer.Option(help="End date for the download")],
    use_ingestion_date: Annotated[
        bool,
        typer.Option(
            "--ingestion-date",
            help="Use ingestion date into SDC database, rather than science measurement date",
        ),
    ] = False,
    level: Annotated[Level, typer.Option(help="Level to download")] = Level.level_2,
    modes: Annotated[
        list[ScienceMode],
        typer.Option(
            help="Science modes to download",
        ),
    ] = [
        "norm",  # type: ignore
        "burst",  # type: ignore
    ],  # for some reason Typer does not like these being enums -
    sensors: Annotated[list[MAGSensor], typer.Option(help="Sensors to download")] = [
        MAGSensor.IBS,
        MAGSensor.OBS,
    ],
    config: Annotated[Path, typer.Option()] = Path("config.yaml"),
) -> dict[Path, SDCMetadataProvider]:
    """Download science data from the SDC."""

    configFile: appConfig.CommandConfigBase = commandInit(config)

    if not auth_code:
        logger.critical("No SDC_AUTH_CODE API key provided")
        raise ValueError("No SDC_AUTH_CODE API key provided")

    logger.info(f"Downloading {level.value} science from {start_date} to {end_date}.")

    data_access = SDCDataAccess(
        data_dir=configFile.work_folder,
        sdc_url=configFile.api.sdc_url if configFile.api else None,
    )

    fetch_science = FetchScience(data_access, modes=modes, sensors=sensors)
    downloaded_science: dict[Path, SDCMetadataProvider] = (
        fetch_science.download_latest_science(
            level=level.value,
            start_date=start_date,
            end_date=end_date,
            use_ingestion_date=use_ingestion_date,
        )
    )

    if not downloaded_science:
        logger.info(
            f"No data downloaded for packet {level.value} from {start_date} to {end_date}."
        )

    output_manager = appUtils.getOutputManager(configFile.destination)
    output_binaries: dict[Path, SDCMetadataProvider] = dict()

    for file, metadata_provider in downloaded_science.items():
        (output_file, output_metadata) = output_manager.add_file(
            file, metadata_provider
        )
        output_binaries[output_file] = output_metadata

    return output_binaries
