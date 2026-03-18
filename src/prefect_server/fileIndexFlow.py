import logging
from datetime import datetime

from prefect import flow
from prefect.states import Completed

from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    IndexByDateRangeRunParameters,
    IndexByFileNamesRunParameters,
    IndexByIdsRunParameters,
    PipelineRunParameters,
    ProgressUpdateMode,
)
from imap_mag.data_pipelines.FileIndexPipeline import FileIndexPipeline
from imap_mag.db import Database
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import try_get_prefect_logger

logger = logging.getLogger(__name__)


def _build_run_parameters(
    files: list[int] | None,
    file_paths: list[str] | None,
    modified_after: datetime | None,
    modified_before: datetime | None,
) -> PipelineRunParameters:
    """Build run parameters from flow inputs.

    Manual inputs take priority in this order: file IDs > file paths > date range.
    If no manual inputs are given, uses AutomaticRunParameters (progress-based).
    """
    if files:
        return IndexByIdsRunParameters(
            file_ids=files,
            progress_mode=ProgressUpdateMode.NEVER_UPDATE_PROGRESS,
        )
    if file_paths:
        return IndexByFileNamesRunParameters(
            file_paths=file_paths,
            progress_mode=ProgressUpdateMode.NEVER_UPDATE_PROGRESS,
        )
    if modified_after is not None or modified_before is not None:
        return IndexByDateRangeRunParameters(
            modified_after=modified_after,
            modified_before=modified_before,
            progress_mode=ProgressUpdateMode.NEVER_UPDATE_PROGRESS,
        )
    return AutomaticRunParameters()


@flow(name=PREFECT_CONSTANTS.FLOW_NAMES.FILE_INDEX, log_prints=True)
async def file_index_flow(
    files: list[int] | None = None,
    file_paths: list[str] | None = None,
    modified_after: datetime | None = None,
    modified_before: datetime | None = None,
):
    """Index metadata about data files (CSV and CDF) into the file_index database table.

    This flow:
    1. Gets files to index from the database
    2. For each file, extracts metadata (record count, timestamps, gaps, bad data, missing data)
    3. Saves metadata to the file_index table (FK to existing files table)

    Args:
        files: Optional list of specific file IDs to reindex.
        file_paths: Optional list of file paths or fnmatch patterns to reindex.
        modified_after: Index files modified after this datetime (manual date range run).
        modified_before: Index files modified before this datetime (manual date range run).

        If none of the above are provided, the flow runs automatically, indexing
        files modified since the last workflow progress timestamp.
    """
    flow_logger = try_get_prefect_logger(__name__)

    app_settings = AppSettings()  # type: ignore
    db = Database()

    run_params = _build_run_parameters(
        files, file_paths, modified_after, modified_before
    )
    flow_logger.info(
        f"Starting file index flow with {type(run_params).__name__}: {run_params}"
    )

    pipeline = FileIndexPipeline(database=db, settings=app_settings)
    pipeline.build(run_parameters=run_params)
    await pipeline.run()

    results = pipeline.get_results()
    indexed_count = len(results.data_items)

    flow_logger.info(f"File index flow completed: {indexed_count} file(s) indexed")
    return Completed(message=f"{indexed_count} file(s) indexed")
