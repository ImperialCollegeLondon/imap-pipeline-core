"""Program to retrieve spin table files from SDC."""

import asyncio
import logging
from datetime import datetime

from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.client.SDCDataAccess import SDCDataAccess
from imap_mag.config import AppSettings
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    FetchByDatesRunParameters,
)
from imap_mag.data_pipelines.SpinTablePipeline import SpinTablePipeline
from imap_mag.db import Database

logger = logging.getLogger(__name__)


def fetch_spin_tables(
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    use_database: bool = False,
):
    """Download spin table files from the SDC.

    Args:
        start_date: Start date for ingestion date filter
        end_date: End date for ingestion date filter
        use_database: Whether to use the database for progress tracking and file indexing
    """

    app_settings = AppSettings()
    work_folder = app_settings.setup_work_folder_for_command(app_settings.fetch_spice)
    initialiseLoggingForCommand(work_folder)

    database = Database() if use_database else None

    client = SDCDataAccess(
        auth_code=app_settings.fetch_spice.api.auth_code,
        data_dir=work_folder,
        sdc_url=app_settings.fetch_spice.api.url_base,
    )

    if start_date or end_date:
        run_params = FetchByDatesRunParameters(
            start_date=start_date,
            end_date=end_date,
        )
    else:
        run_params = AutomaticRunParameters()

    pipeline = SpinTablePipeline(
        database=database, settings=app_settings, client=client
    )
    pipeline.build(run_params)
    asyncio.run(pipeline.run())

    result = pipeline.get_results()
    if not result.success:
        raise RuntimeError(f"Pipeline failed: {result}")

    logger.info(
        f"Spin table download complete. {len(result.data_items)} files processed."
    )
