import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.config import AppSettings, FetchMode
from imap_mag.data_pipelines import AutomaticRunParameters, FetchByDatesRunParameters
from imap_mag.data_pipelines.IALiRTInstrumentPipeline import IALiRTInstrumentPipeline
from imap_mag.db import Database
from imap_mag.util.constants import CONSTANTS

logger = logging.getLogger(__name__)

# Instruments accepted by `fetch ialirt` (excludes mag_hk which has its own sub-command)
_SCIENCE_INSTRUMENTS = [
    k for k in CONSTANTS.DATABASE.IALIRT_INSTRUMENT_PROGRESS_IDS if k != "mag_hk"
]


def _run_pipeline(
    instrument: str,
    start_date: datetime | None,
    end_date: datetime | None,
    fetch_mode: FetchMode,
    app_settings: AppSettings,
) -> list[Path]:
    """Build and run an IALiRTInstrumentPipeline, returning the produced file paths."""

    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_ialirt)
    initialiseLoggingForCommand(work_folder)

    use_database = fetch_mode == FetchMode.DownloadAndUpdateProgress
    database = Database() if use_database else None

    pipeline = IALiRTInstrumentPipeline(
        instrument=instrument,
        database=database,
        settings=app_settings,
    )

    if start_date is not None:
        run_params: AutomaticRunParameters | FetchByDatesRunParameters = (
            FetchByDatesRunParameters(start_date=start_date, end_date=end_date)
        )
    else:
        run_params = AutomaticRunParameters()

    pipeline.build(run_params)
    asyncio.run(pipeline.run())
    result = pipeline.get_results()

    if not result.success:
        raise RuntimeError(f"I-ALiRT {instrument} pipeline failed: {result}")

    return [item.file_path for item in result.data_items if hasattr(item, "file_path")]


# E.g.,
# imap-mag fetch ialirt --start-date 2025-01-02 --end-date 2025-01-03
# imap-mag fetch ialirt --instrument swe --start-date 2025-01-02 --end-date 2025-01-03
def fetch_ialirt(
    start_date: Annotated[
        datetime | None,
        typer.Option(help="Start date for the download"),
    ] = None,
    end_date: Annotated[
        datetime | None,
        typer.Option(help="End date for the download"),
    ] = None,
    instrument: Annotated[
        str,
        typer.Option(
            help=f"I-ALiRT instrument to download. Supported: {', '.join(_SCIENCE_INSTRUMENTS)}",
        ),
    ] = "mag",
    fetch_mode: Annotated[
        FetchMode,
        typer.Option(
            case_sensitive=False,
            help="Whether to download only or download and update progress in database",
        ),
    ] = FetchMode.DownloadOnly,
) -> list[Path]:
    """Download I-ALiRT data from the I-ALiRT API."""

    if instrument not in _SCIENCE_INSTRUMENTS:
        raise typer.BadParameter(
            f"Unknown instrument '{instrument}'. Supported: {', '.join(_SCIENCE_INSTRUMENTS)}"
        )

    app_settings = AppSettings()  # type: ignore

    files = _run_pipeline(instrument, start_date, end_date, fetch_mode, app_settings)

    if not files:
        logger.info(f"No I-ALiRT {instrument} data downloaded.")
    else:
        logger.debug(
            f"Downloaded {len(files)} files:\n{', '.join(str(f) for f in files)}"
        )

    return files


# E.g.,
# imap-mag fetch ialirt-hk --start-date 2025-01-02 --end-date 2025-01-03
def fetch_ialirt_hk(
    start_date: Annotated[
        datetime | None,
        typer.Option(help="Start date for the download"),
    ] = None,
    end_date: Annotated[
        datetime | None,
        typer.Option(help="End date for the download"),
    ] = None,
    fetch_mode: Annotated[
        FetchMode,
        typer.Option(
            case_sensitive=False,
            help="Whether to download only or download and update progress in database",
        ),
    ] = FetchMode.DownloadOnly,
) -> list[Path]:
    """Download I-ALiRT MAG HK data from the I-ALiRT API."""

    app_settings = AppSettings()  # type: ignore

    files = _run_pipeline("mag_hk", start_date, end_date, fetch_mode, app_settings)

    if not files:
        logger.info("No I-ALiRT MAG HK data downloaded.")
    else:
        logger.debug(
            f"Downloaded {len(files)} files:\n{', '.join(str(f) for f in files)}"
        )

    return files
