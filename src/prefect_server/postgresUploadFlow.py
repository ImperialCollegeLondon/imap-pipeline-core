import fnmatch
import os
import re
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from crump import CrumpConfig, sync_file_to_db
from prefect import flow, get_run_logger
from prefect.states import Completed

from imap_mag.config.AppSettings import AppSettings
from imap_mag.db import Database
from prefect_server.constants import PREFECT_CONSTANTS

PROGRESS_KEY = "postgres-upload"


def extract_version_and_date(file_path: Path) -> tuple[datetime | None, int]:
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
                tzinfo=timezone.utc,
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
                    tzinfo=timezone.utc,
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


def select_latest_version_per_day(files: list) -> list:
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
        date, version = extract_version_and_date(Path(file.path))
        date_key = date.date() if date else None
        files_by_date[date_key].append((file, version))

    # Select latest version per date
    latest_files = []
    for date_key, file_list in files_by_date.items():
        # Sort by version (descending) and take the first one
        file_list.sort(key=lambda x: x[1], reverse=True)
        latest_files.append(file_list[0][0])  # Append the file object

    return latest_files


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.POSTGRES_UPLOAD,
    log_prints=True,
)
async def upload_new_files_to_postgres(
    find_files_after: datetime | None = None,
    how_many: int | None = None,
    job_name: str | None = None,
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
        how_many: Optional limit on number of files to process
        job_name: Optional specific job name from crump config to use.
                 If None, will auto-detect if only one job exists in config.
    """

    logger = get_run_logger()

    app_settings = AppSettings()  # type: ignore
    db = Database()
    started = datetime.now(tz=timezone.utc)

    # Get workflow progress
    workflow_progress = db.get_workflow_progress(PROGRESS_KEY)
    if workflow_progress.progress_timestamp is None:
        workflow_progress.progress_timestamp = datetime(2010, 1, 1, tzinfo=timezone.utc)

    last_modified_date = (
        workflow_progress.progress_timestamp
        if find_files_after is None
        else find_files_after
    )

    logger.info(
        f"Looking for {how_many if how_many else 'all'} files modified after {last_modified_date}"
    )

    # Get new files from database
    new_files_db = db.get_files_since(last_modified_date, how_many)

    workflow_progress.update_last_checked_date(started)

    logger.info(
        f"Found {len(new_files_db)} new files. Checking against {len(app_settings.postgres_upload.paths_to_match)} patterns from settings."
    )

    # Filter files by patterns
    files = [
        f
        for f in new_files_db
        if any(
            fnmatch.fnmatch(f.path, p)
            for p in app_settings.postgres_upload.paths_to_match
        )
    ]

    logger.info(f"{len(files)} files matching upload patterns.")

    if files:
        # Select only latest version per day
        files = select_latest_version_per_day(files)
        logger.info(
            f"After selecting latest version per day: {len(files)} files to process.\nProcessing: {', '.join(str(f.path) for f in files)}"
        )

    # Get database URL from environment
    db_url_env_var = app_settings.postgres_upload.database_url_env_var
    db_url = os.getenv(db_url_env_var)

    if not db_url:
        logger.error(
            f"Database URL not found in environment variable '{db_url_env_var}'. Skipping upload."
        )
        return Completed(
            message=f"Database URL not configured in {db_url_env_var}",
            name=PREFECT_CONSTANTS.SKIPPED_STATE_NAME,
        )

    # Load crump configuration
    crump_config_path = app_settings.postgres_upload.crump_config_path
    if not crump_config_path.exists():
        logger.error(
            f"Crump configuration file not found at {crump_config_path}. Skipping upload."
        )
        return Completed(
            message=f"Crump config not found at {crump_config_path}",
            name=PREFECT_CONSTANTS.SKIPPED_STATE_NAME,
        )

    logger.info(f"Loading crump configuration from {crump_config_path}")
    crump_config = CrumpConfig.from_yaml(crump_config_path)

    # Determine which job to use
    if job_name is None:
        if len(crump_config.jobs) == 1:
            job_name = list(crump_config.jobs.keys())[0]
            logger.info(f"Auto-detected single job: {job_name}")
        else:
            logger.error(
                f"Multiple jobs found in config ({', '.join(crump_config.jobs.keys())}). "
                "Please specify job_name parameter."
            )
            return Completed(
                message="Multiple jobs in config, job_name required",
                name=PREFECT_CONSTANTS.SKIPPED_STATE_NAME,
            )

    job = crump_config.get_job(job_name)
    logger.info(f"Using crump job '{job_name}' targeting table '{job.target_table}'")

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
                            job=job,
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
                    job=job,
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
        new_progress_date = min(started, latest_file_timestamp.astimezone(timezone.utc))
        workflow_progress.update_progress_timestamp(new_progress_date)
        logger.info(f"Set progress timestamp for {PROGRESS_KEY} to {new_progress_date}")

        message = f"{uploaded_count} file(s) uploaded to PostgreSQL"
        if failed_count > 0:
            message += f", {failed_count} failed"
        result = Completed(message=message)
    else:
        if failed_count > 0:
            result = Completed(message=f"All {failed_count} files failed")
        else:
            result = Completed(
                message="No work to do 💤", name=PREFECT_CONSTANTS.SKIPPED_STATE_NAME
            )

    db.save(workflow_progress)
    logger.info(
        f"PostgreSQL upload complete: {uploaded_count} succeeded, {failed_count} failed"
    )
    return result
