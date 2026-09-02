import asyncio
import logging
from typing import Annotated, Literal

import typer

from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.config import AppSettings, FetchMode
from imap_mag.data_pipelines import AutomaticRunParameters
from imap_mag.data_pipelines.NOAAPipeline import NOAAPipeline

logger = logging.getLogger(__name__)


# E.g.,
# imap-mag fetch noaa --spacecraft SOLAR1 --instrument wind
def fetch_noaa(
    spacecraft: Annotated[
        Literal["SOLAR1", "ACE"],
        typer.Option(
            help="Spacecraft to download data for. Must be 'SOLAR1' or 'ACE'",
        ),
    ],
    instrument: Annotated[
        Literal["mag", "wind"],
        typer.Option(
            help="Instrument data to download. Must be 'mag' for the magnetic field instrument or 'wind' for the plasma instrument",
        ),
    ],
    fetch_mode: Annotated[
        FetchMode,
        typer.Option(
            help="Whether to download only or download and update progress in database",
        ),
    ] = FetchMode.DownloadOnly,
) -> None:
    """Download SOLAR1 and ACE data from NOAA."""

    app_settings = AppSettings()  #  type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_spice)
    initialiseLoggingForCommand(work_folder)

    pipeline = NOAAPipeline(
        spacecraft=spacecraft,
        instrument=instrument,
        database=None,
        settings=app_settings,
    )
    run_params = AutomaticRunParameters()

    pipeline.build(run_params)
    asyncio.run(pipeline.run())

    result = pipeline.get_results()
    if not result.success:
        raise RuntimeError(f"Pipeline failed: {result}")

    logger.info(
        f"NOAA data download complete. {len(result.data_items)} files processed."
    )
