import fnmatch
import logging
import os
import re
import tempfile
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

from crump import CrumpConfig, sync_file_to_db
from prefect import flow, get_run_logger
from prefect.states import Completed, Failed
from prefect_sqlalchemy import SqlAlchemyConnector

from imap_db.model import File
from imap_mag.config.AppSettings import AppSettings
from imap_mag.db import Database
from prefect_server.constants import PREFECT_CONSTANTS

logger = logging.getLogger(__name__)


def _extract_version_and_date(file_path: Path) -> tuple[datetime | None, int]:
    """
    Extract date and version number from a file path.

    Looks for patterns like:
    - YYYY-MM-DD or YYYYMMDD for dates
    - v###, version###, _###_ for version numbers

    Args:
        file_path: Path to the file

    Returns:
        Tuple of (date, version) where date is a datetime object or None,
        and version is an integer (0 if not found)
    """
    path_str = str(file_path)

    # Extract date - try multiple patterns
    date = None
    # Pattern 1: YYYY-MM-DD
    date_match = re.search(r"(\d{4})-(\d{2})-(\d{2})", path_str)
    if date_match:
        try:
            date = datetime(
                int(date_match.group(1)),
                int(date_match.group(2)),
                int(date_match.group(3)),
                tzinfo=UTC,
            )
        except ValueError:
            pass

    # Pattern 2: YYYYMMDD
    if date is None:
        date_match = re.search(r"(\d{4})(\d{2})(\d{2})", path_str)
        if date_match:
            try:
                date = datetime(
                    int(date_match.group(1)),
                    int(date_match.group(2)),
                    int(date_match.group(3)),
                    tzinfo=UTC,
                )
            except ValueError:
                pass

    # Extract version number - try multiple patterns
    version = 0
    # Pattern 1: v### or version###
    version_match = re.search(r"v(?:ersion)?[\s_-]?(\d+)", path_str, re.IGNORECASE)
    if version_match:
        version = int(version_match.group(1))
    else:
        # Pattern 2: _###_ or -###- (version between separators)
        version_match = re.search(r"[_-](\d{3,})[_-]", path_str)
        if version_match:
            version = int(version_match.group(1))

    return date, version


def _select_latest_version_per_day(files: list[File]) -> list[File]:
    """
    Select only the latest version of files per day.

    Groups files by date and selects the file with the highest version number
    for each date. Files without dates are kept separate and the latest version
    among them is selected.

    Args:
        files: List of File objects from database

    Returns:
        List of File objects containing only the latest version per day
    """
    # Group files by date
    files_by_date = defaultdict(list)

    for file in files:
        date, version = _extract_version_and_date(Path(file.path))
        type_date_key = (file.get_file_type_string(), date.date() if date else None)
        files_by_date[type_date_key].append((file, version))

    # Select latest version per date
    latest_files = []
    for _, file_list in files_by_date.items():
        # Sort by version (descending) and take the first one
        file_list.sort(key=lambda x: x[1], reverse=True)
        latest_files.append(file_list[0][0])  # Append the file object

    return latest_files


async def _get_database_connectionstring(
    app_settings: AppSettings,
    db_env_name_or_block_name_or_block: str | SqlAlchemyConnector | None,
) -> str:
    """
    Get database connection string from environment variable or Prefect block.

    Args:
        db_env_name_or_block_name_or_block: Environment variable name, Prefect block name, or SqlAlchemyConnector block.
    Returns:
        Database connection string.
    """

    if db_env_name_or_block_name_or_block is None:
        logger.info("Using database connection info from app settings")
        db_url_lookup_key = (
            app_settings.postgres_upload.database_url_env_var_or_block_name
        )
    else:
        db_url_lookup_key = db_env_name_or_block_name_or_block

    if db_url_lookup_key is None:
        raise RuntimeError(
            "Database connection information not provided. Cannot upload."
        )

    db_url = None

    if isinstance(db_env_name_or_block_name_or_block, SqlAlchemyConnector):
        db_url = db_env_name_or_block_name_or_block._rendered_url.render_as_string(
            False
        )
    elif isinstance(db_env_name_or_block_name_or_block, str):
        # Check if it's an environment variable
        env_value = os.getenv(db_env_name_or_block_name_or_block)
        if env_value:
            logger.info(
                f"Using database connection string from environment variable {db_env_name_or_block_name_or_block}"
            )
            db_url = env_value
        else:
            # Assume it's a Prefect block name
            try:
                connector_block = await SqlAlchemyConnector.aload(
                    db_env_name_or_block_name_or_block
                )
                logger.info(
                    f"Using database connection string from Prefect SqlAlchemyConnector block {db_env_name_or_block_name_or_block}\n{connector_block._rendered_url.render_as_string(True)}"
                )
                db_url = connector_block._rendered_url.render_as_string(False)
            except ValueError:
                logger.info(
                    f"{db_env_name_or_block_name_or_block} SqlAlchemyConnector block not found or empty"
                )

    if db_url is None:
        raise ValueError("Invalid database connection input")

    # Convert PostgreSQL URL for crump (remove +psycopg driver specification)
    # crump expects/needs: postgresql://user:pass@host:port/dbname
    db_url = db_url.replace("postgresql+psycopg://", "postgresql://")

    return db_url


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POSTGRES_UPLOAD,
    log_prints=True,
)
async def upload_new_files_to_postgres(
    find_files_after: datetime | None = None,
    paths_to_match: list[str] | None = None,
    how_many: int | None = None,
    job_name: str | None = None,
    db_env_name_or_block_name_or_block: str
    | SqlAlchemyConnector
    | None = PREFECT_CONSTANTS.IMAP_DATABASE_BLOCK_NAME,
    progress_key="postgres-upload",
):
    """
    Upload new CSV and CDF files to PostgreSQL database using crump.

    This flow:
    1. Finds new/modified files since last run (or since find_files_after)
    2. Filters files based on configured patterns
    3. Selects only the latest version per day
    4. Uses crump to sync files to PostgreSQL database based on config

    Args:
        find_files_after: Optional datetime to find files modified after this time.
                         If None, uses the last progress timestamp.
        paths_to_match: Optional list of path patterns to filter files.
                        If None, uses patterns from app settings.
        how_many: Optional limit on number of files to process
        job_name: Optional specific job name from crump config to use.
                 If None, will auto-detect from file names patterns configured in the crump config file.
    """

    logger = get_run_logger()

    app_settings = AppSettings()  # type: ignore
    db = Database()  # the IMAP database to track progress - could be different from the target Postgres database
    started = datetime.now(tz=UTC)

    db_url = await _get_database_connectionstring(
        app_settings, db_env_name_or_block_name_or_block
    )

    # Get workflow progress
    workflow_progress = db.get_workflow_progress(progress_key)
    if workflow_progress.progress_timestamp is None:
        workflow_progress.progress_timestamp = datetime(2010, 1, 1, tzinfo=UTC)

    last_modified_date = (
        workflow_progress.progress_timestamp
        if find_files_after is None
        else find_files_after
    )

    paths_to_match = (
        paths_to_match
        if paths_to_match is not None
        else app_settings.postgres_upload.paths_to_match
    )

    logger.info(
        f"Looking for {how_many if how_many else 'all'} files modified after {last_modified_date} matching patterns: {paths_to_match}"
    )

    # Get new files from database
    new_files_db = db.get_files_since(last_modified_date, how_many)

    workflow_progress.update_last_checked_date(started)

    # Filter files by patterns
    files = [
        f
        for f in new_files_db
        if any(fnmatch.fnmatch(f.path, p) for p in paths_to_match)
    ]
    logger.info(
        f"Found {len(new_files_db)} new files. Checked against {len(paths_to_match)} patterns from settings and {len(files)} files match"
    )

    if files:
        # Select only latest version per day
        files = _select_latest_version_per_day(files)
        logger.info(
            f"After selecting latest version per day: {len(files)} files to process.\nProcessing: {', '.join(str(f.path) for f in files)}"
        )

    # Load crump configuration
    crump_config_path = app_settings.postgres_upload.crump_config_path
    logger.info(f"Loading crump config path: {crump_config_path.absolute()}")
    if not crump_config_path.exists():
        raise ValueError(
            f"Crump configuration file not found at {crump_config_path}. Skipping upload."
        )
    crump_config = CrumpConfig.from_yaml(crump_config_path)

    # Process each file
    uploaded_count = 0
    failed_count = 0

    for file in files:
        path_inside_datastore = Path(file.path)
        if app_settings.data_store in Path(file.path).parents:
            path_inside_datastore = path_inside_datastore.absolute().relative_to(
                app_settings.data_store.absolute()
            )
        path_inc_datastore = app_settings.data_store / path_inside_datastore

        if not path_inc_datastore.exists():
            logger.warning(
                f"File {path_inside_datastore} does not exist, skipping upload."
            )
            failed_count += 1
            continue

        # Determine job to use
        try:
            logger.info(
                f"Determining crump job for file {path_inc_datastore.as_posix()} and name {job_name}..."
            )
            detected_crump_job_details = crump_config.get_job_or_auto_detect(
                job_name, filename=path_inc_datastore.as_posix()
            )
            if detected_crump_job_details is None:
                raise ValueError("No matching job found in crump config")

            detected_crump_job, detected_crump_job_name = detected_crump_job_details

            logger.info(
                f"Using crump job '{detected_crump_job_name}' targeting table '{detected_crump_job.target_table}'"
            )
        except ValueError as ve:
            logger.error(
                f"Failed to determine crump job for {path_inside_datastore}: {ve}"
            )
            failed_count += 1
            continue

        try:
            logger.info(f"Syncing {path_inside_datastore} to database...")

            # Handle CDF files by extracting to temporary CSV files first
            if path_inc_datastore.suffix.lower() in [".cdf"]:
                with tempfile.TemporaryDirectory() as temp_dir:
                    from crump.cdf_extractor import extract_cdf_to_tabular_file

                    logger.info(f"Extracting CDF file {path_inc_datastore}...")

                    # Extract CDF to CSV
                    results = extract_cdf_to_tabular_file(
                        cdf_file_path=path_inc_datastore,
                        output_dir=Path(temp_dir),
                        filename_template=f"{path_inc_datastore.stem}_[VARIABLE_NAME].csv",
                        automerge=True,
                        append=False,
                        variable_names=None,
                        max_records=app_settings.postgres_upload.max_records_per_cdf,
                    )

                    logger.info(
                        f"Extracted {len(results)} CSV file(s) from CDF, syncing to database..."
                    )

                    # Sync each extracted CSV
                    for result in results:
                        rows_synced = sync_file_to_db(
                            file_path=result.output_file,
                            job=detected_crump_job,
                            db_connection_string=db_url,
                            enable_history=app_settings.postgres_upload.enable_history,
                        )
                        logger.info(
                            f"  Synced {rows_synced} rows from {result.output_file.name}"
                        )
            else:
                # Direct sync for CSV and Parquet files
                rows_synced = sync_file_to_db(
                    file_path=path_inc_datastore,
                    job=detected_crump_job,
                    db_connection_string=db_url,
                    enable_history=app_settings.postgres_upload.enable_history,
                )
                logger.info(f"Synced {rows_synced} rows from {path_inside_datastore}")

            uploaded_count += 1

        except Exception as e:
            logger.error(f"Failed to sync {path_inside_datastore}: {e}")
            failed_count += 1
            continue

    # Update progress
    result = None
    if uploaded_count > 0:
        logger.info(f"Upload completed: {uploaded_count} files synced to database")
        latest_file_timestamp = max(f.last_modified_date for f in files)
        workflow_progress.update_progress_timestamp(
            latest_file_timestamp.astimezone(UTC)
        )
        logger.info(
            f"Set progress timestamp for {progress_key} to {latest_file_timestamp.astimezone(UTC)}"
        )

        message = f"{uploaded_count} file(s) uploaded to PostgreSQL"
        if failed_count > 0:
            message += f", {failed_count} failed"
        result = Completed(message=message)
    else:
        if failed_count > 0:
            result = Failed(message=f"All {failed_count} files failed")
        else:
            result = Completed(
                message="No work to do 💤", name=PREFECT_CONSTANTS.SKIPPED_STATE_NAME
            )

    db.save(workflow_progress)
    logger.info(
        f"PostgreSQL upload complete: {uploaded_count} succeeded, {failed_count} failed"
    )
    return result
