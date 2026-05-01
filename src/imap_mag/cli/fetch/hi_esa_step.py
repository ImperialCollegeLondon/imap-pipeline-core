"""Programs to retrieve IMAP-Hi ESA STEP housekeeping CSV files from WebTCAD LaTiS."""

import asyncio
import logging
from datetime import datetime

from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.client.WebTCADLaTiS import HKWebTCADItems
from imap_mag.config import AppSettings
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    FetchByDatesRunParameters,
)
from imap_mag.data_pipelines.HiEsaStepPipeline import HiEsaStepPipeline
from imap_mag.db import Database

logger = logging.getLogger(__name__)


def _run(
    item: HKWebTCADItems,
    start_date: datetime | None,
    end_date: datetime | None,
    use_database: bool,
):
    app_settings = AppSettings()
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_webtcad)
    initialiseLoggingForCommand(work_folder)

    database = Database() if use_database else None

    if start_date or end_date:
        run_params = FetchByDatesRunParameters(
            start_date=start_date,
            end_date=end_date,
        )
    else:
        run_params = AutomaticRunParameters()

    pipeline = HiEsaStepPipeline(item=item, database=database, settings=app_settings)
    pipeline.build(run_params)
    asyncio.run(pipeline.run())

    result = pipeline.get_results()
    if not result.success:
        raise RuntimeError(f"Pipeline failed: {result}")

    logger.info(
        f"{item.name} download complete. {len(result.data_items)} files processed."
    )


def fetch_hi45_esa_step(
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    use_database: bool = False,
):
    """Download IMAP-Hi 45 ESA STEP housekeeping CSV data from WebTCAD LaTiS."""
    _run(HKWebTCADItems.HI45_ESA_STEP, start_date, end_date, use_database)


def fetch_hi90_esa_step(
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    use_database: bool = False,
):
    """Download IMAP-Hi 90 ESA STEP housekeeping CSV data from WebTCAD LaTiS."""
    _run(HKWebTCADItems.HI90_ESA_STEP, start_date, end_date, use_database)
