"""Prefect flow for cleaning up files from the datastore."""

import logging
import shutil
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

from prefect import flow, get_run_logger
from prefect.states import Completed

from imap_db.model import File
from imap_mag.config.AppSettings import AppSettings
from imap_mag.config.DatastoreCleanupConfig import CleanupMode, CleanupTask
from imap_mag.db import Database
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.durationUtils import parse_duration
from prefect_server.fileVersionUtils import (
    get_file_type_date_key,
    get_file_version,
)

logger = logging.getLogger(__name__)


def _identify_non_latest_versions(files: list[File]) -> list[File]:
    """
    Identify files that are not the latest version for their file type and date.

    Args:
        files: List of File objects from database

    Returns:
        List of File objects that are NOT the latest version
    """
    files_by_type_date: dict[tuple[str, datetime | None], list[tuple[File, int]]] = (
        defaultdict(list)
    )

    for file in files:
        type_date_key = get_file_type_date_key(file)
        version = get_file_version(file)
        files_by_type_date[type_date_key].append((file, version))

    non_latest_files = []
    for _, file_list in files_by_type_date.items():
        if len(file_list) <= 1:
            continue

        file_list.sort(key=lambda x: x[1], reverse=True)
        for file, _ in file_list[1:]:
            non_latest_files.append(file)

    return non_latest_files


def _get_files_matching_patterns(
    db: Database,
    patterns: list[str],
) -> list[File]:
    """
    Get all active files matching any of the given patterns.

    Queries the database server-side to avoid loading all files into memory.

    Args:
        db: Database instance
        patterns: List of fnmatch patterns

    Returns:
        List of matching files
    """
    return db.get_active_files_matching_patterns(patterns)


def _get_files_to_cleanup(
    files: list[File],
    task: CleanupTask,
    age_cutoff: datetime,
) -> list[File]:
    """
    Get files that should be cleaned up based on task configuration.

    Args:
        files: Files matching the task's path patterns
        task: Cleanup task configuration
        age_cutoff: Files older than this will be considered for cleanup

    Returns:
        List of files to clean up
    """
    if not files:
        return []

    # If keep_latest_version_only, only consider non-latest versions
    if task.keep_latest_version_only:
        candidates = _identify_non_latest_versions(files)
    else:
        candidates = files

    # Filter by age
    files_to_cleanup = []
    for f in candidates:
        file_modified = f.last_modified_date
        if file_modified.tzinfo is None:
            file_modified = file_modified.replace(tzinfo=UTC)
        if file_modified < age_cutoff:
            files_to_cleanup.append(f)

    return files_to_cleanup


def _archive_file(
    file: File,
    datastore: Path,
    archive_folder: Path,
    db: Database,
    archive_date: datetime,
) -> None:
    """
    Move a file to the archive folder.

    1. Copy to archive location
    2. Create new database record for archived file
    3. Mark original file as deleted
    4. Delete original file from filesystem

    Args:
        file: File to archive
        datastore: Path to datastore root
        archive_folder: Path to archive folder
        db: Database instance
        archive_date: Timestamp to record as archive/deletion date
    """
    source_path = datastore / file.path
    dest_path = archive_folder / file.path

    # Create destination directory
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    # Copy file to archive
    shutil.copy2(source_path, dest_path)

    # Create new database record for archived file
    new_db_path = str(dest_path.relative_to(archive_folder.parent))
    archived_file = File(
        name=file.name,
        path=new_db_path,
        version=file.version,
        hash=file.hash,
        size=file.size,
        content_date=file.content_date,
        creation_date=archive_date,
        last_modified_date=archive_date,
        software_version=file.software_version,
    )
    db.insert_file(archived_file)

    # Mark original file as deleted
    db.mark_file_as_deleted(file, archive_date)

    # Delete original from filesystem
    source_path.unlink()


def _delete_file(
    file: File,
    datastore: Path,
    db: Database,
    deletion_date: datetime,
) -> None:
    """
    Delete a file and mark it as deleted in the database.

    Args:
        file: File to delete
        datastore: Path to datastore root
        db: Database instance
        deletion_date: Timestamp to record as deletion date
    """
    file_path = datastore / file.path

    # Mark as deleted in database first
    db.mark_file_as_deleted(file, deletion_date)

    # Delete from filesystem if it exists
    if file_path.exists():
        file_path.unlink()


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.DATASTORE_CLEANUP,
    log_prints=True,
)
async def cleanup_datastore(
    task_names: list[str] | None = None,
    dry_run: bool | None = None,
    max_file_operations: int = 100,
):
    """
    Clean up files from the datastore based on configured tasks.

    Each cleanup task can specify:
    - paths_to_match: File patterns to match
    - files_older_than: Files older than this duration will be considered
    - keep_latest_version_only: Only remove non-latest versions
    - cleanup_mode: 'delete' or 'archive'
    - archive_folder: Where to archive files (required if cleanup_mode is 'archive')

    Args:
        task_names: Optional list of task names to run. If None, runs all tasks.
        dry_run: If True, only log what would happen. If None, uses config value.
        max_file_operations: Maximum number of file operations (archive/delete)
            before stopping. Default is 100.
    """
    logger = get_run_logger()

    app_settings = AppSettings()  # type: ignore
    db = Database()
    started = datetime.now(tz=UTC)

    # Get configuration
    config = app_settings.datastore_cleanup
    dry_run = dry_run if dry_run is not None else config.dry_run

    # Filter tasks if specific names requested
    tasks = config.tasks
    if task_names:
        tasks = [t for t in tasks if t.name in task_names]
        if not tasks:
            return Completed(
                message=f"No tasks found matching: {task_names}",
                name=PREFECT_CONSTANTS.SKIPPED_STATE_NAME,
            )

    logger.info(
        f"Running {len(tasks)} cleanup task(s): {[t.name for t in tasks]}. "
        f"Dry run: {dry_run}, max operations: {max_file_operations}"
    )

    total_deleted = 0
    total_archived = 0
    operations_performed = 0

    for task in tasks:
        if operations_performed >= max_file_operations:
            logger.info(
                f"Reached max file operations limit ({max_file_operations}). "
                "Stopping cleanup."
            )
            break

        logger.info(f"Processing task: {task.name}")

        # Parse files_older_than duration
        files_older_than = parse_duration(task.files_older_than)
        age_cutoff = started - files_older_than

        # Get files matching this task's patterns
        matched_files = _get_files_matching_patterns(db, task.paths_to_match)

        if not matched_files:
            logger.info(f"  No files match patterns for task '{task.name}'")
            continue

        logger.info(f"  Found {len(matched_files)} files matching patterns")

        # Get files to clean up
        files_to_cleanup = _get_files_to_cleanup(matched_files, task, age_cutoff)

        if not files_to_cleanup:
            logger.info(f"  No files to clean up for task '{task.name}'")
            continue

        logger.info(
            f"  {len(files_to_cleanup)} files to clean up "
            f"(files_older_than={task.files_older_than}, keep_latest_only={task.keep_latest_version_only})"
        )

        for file in files_to_cleanup:
            if operations_performed >= max_file_operations:
                logger.info(
                    f"Reached max file operations limit ({max_file_operations}). "
                    "Stopping cleanup."
                )
                break

            if task.cleanup_mode == CleanupMode.ARCHIVE:
                action = f"archive to {task.archive_folder}"
            else:
                action = "delete"

            if dry_run:
                logger.info(f"  [DRY RUN] Would {action}: {file.path}")
                operations_performed += 1
                if task.cleanup_mode == CleanupMode.ARCHIVE:
                    total_archived += 1
                else:
                    total_deleted += 1
                continue

            # Perform actual cleanup
            if task.cleanup_mode == CleanupMode.ARCHIVE:
                _archive_file(
                    file,
                    app_settings.data_store,
                    task.archive_folder,
                    db,
                    started,
                )
                logger.info(f"  Archived: {file.path}")
                total_archived += 1
            else:
                _delete_file(file, app_settings.data_store, db, started)
                logger.info(f"  Deleted: {file.path}")
                total_deleted += 1

            operations_performed += 1

    # Determine result
    action_word = "would be" if dry_run else "were"
    parts = []
    if total_deleted > 0:
        parts.append(f"{total_deleted} deleted")
    if total_archived > 0:
        parts.append(f"{total_archived} archived")

    if operations_performed >= max_file_operations:
        parts.append(f"stopped at limit ({max_file_operations})")

    if parts:
        message = f"Files {action_word}: " + ", ".join(parts)
        return Completed(message=message)
    else:
        return Completed(
            message="No files to clean up 💤",
            name=PREFECT_CONSTANTS.SKIPPED_STATE_NAME,
        )
