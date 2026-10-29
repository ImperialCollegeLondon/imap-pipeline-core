import logging
from typing import Annotated

from prefect import flow
from prefect.states import Completed
from pydantic import Field

from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    IndexByIdsRunParameters,
    IndexByPathOrModifiedDateRunParameters,
)
from imap_mag.data_pipelines.FileAnalysisPipeline import FileAnalysisPipeline
from imap_mag.db import Database
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import try_get_prefect_logger

logger = logging.getLogger(__name__)


@flow(name=PREFECT_CONSTANTS.FLOW_NAMES.FILE_ANALYSER, log_prints=True)
async def file_analyser_flow(
    run_parameters: Annotated[
        AutomaticRunParameters
        | IndexByIdsRunParameters
        | IndexByPathOrModifiedDateRunParameters,
        Field(
            json_schema_extra={
                "title": "Run parameters",
                "description": "Parameters for the pipeline run. Automatic indexes files modified since the last workflow progress. Alternatively index specific files by ID, or by path patterns and/or modified date range.",
            }
        ),
    ] = AutomaticRunParameters(),
    max_files_per_run: Annotated[
        int | None,
        Field(
            json_schema_extra={
                "title": "Max files per run",
                "description": "Maximum number of files to analyse in an automatic run. Overrides the value in the config file if provided.",
            }
        ),
    ] = None,
):
    """Index metadata about CSV, Parquet and CDF files into the file_analysis database table.

    This flow:
    1. Gets files to index from the database
    2. For each file, extracts metadata (record count, timestamps, gaps, bad data, missing data)
    3. Saves metadata to the file_analysis table (FK to existing files table)

    Args:
        run_parameters: Which files to index - automatically (files modified since the
            last workflow progress), by file IDs, or by path patterns and/or modified dates.
        max_files_per_run: Optional override of the max_files_per_run config setting.
    """
    flow_logger = try_get_prefect_logger(__name__)

    app_settings = AppSettings()  # type: ignore
    if max_files_per_run is not None:
        app_settings.file_analysis.max_files_per_run = max_files_per_run

    db = Database()

    flow_logger.info(
        f"Starting file index flow with {type(run_parameters).__name__}: {run_parameters}"
    )

    pipeline = FileAnalysisPipeline(database=db, settings=app_settings)
    pipeline.build(run_parameters=run_parameters)
    await pipeline.run()

    results = pipeline.get_results()
    indexed_count = len(results.data_items)

    flow_logger.info(f"File index flow completed: {indexed_count} file(s) indexed")
    return Completed(message=f"{indexed_count} file(s) indexed")
