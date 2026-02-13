"""Prefect flow for cleaning up files from the datastore."""

import logging
from datetime import UTC

from prefect import flow
from prefect.states import Completed

from imap_db.model import File
from imap_mag.config.AppSettings import AppSettings
from imap_mag.config.DatastoreCleanupConfig import CleanupMode, CleanupTask
from imap_mag.db import Database
from imap_mag.io.DBIndexedDatastoreFileManager import (
    DBIndexedDatastoreFileManager,
)
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import try_get_prefect_logger

logger = logging.getLogger(__name__)


def _identify_non_latest_versions(files: list[File]) -> list[File]:
    all_files = set(files)
    latest_files = set(File.filter_to_latest_versions_only(files))
    non_latest_files = list(all_files - latest_files)

    return non_latest_files


def _get_files_to_cleanup(
    files: list[File],
    task: CleanupTask,
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
    cutoff = task.get_file_age_cutoff()
    files_to_cleanup = []
    for f in candidates:
        file_modified = f.last_modified_date
        if file_modified.tzinfo is None:
            file_modified = file_modified.replace(tzinfo=UTC)
        if file_modified < cutoff:
            files_to_cleanup.append(f)

    return files_to_cleanup


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.DATASTORE_CLEANUP,
)
async def cleanup_datastore_flow(
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
    logger = try_get_prefect_logger(__name__)

    app_settings = AppSettings()  # type: ignore
    db = Database()
    datastore_manager = DBIndexedDatastoreFileManager(
        database=db, settings=app_settings
    )

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
    exiting_early = False

    for task in tasks:
        if exiting_early:
            break
        if operations_performed >= max_file_operations:
            logger.info(
                f"Reached max file operations limit ({max_file_operations}). "
                "Stopping cleanup."
            )
            break

        logger.info(f"Processing task: {task.name}")

        # Get files matching this task's patterns
        matched_files = db.get_active_files_matching_patterns(task.paths_to_match)

        if not matched_files:
            logger.info(f"  No files match patterns for task '{task.name}'")
            continue

        # Get files to clean up
        files_to_cleanup = _get_files_to_cleanup(matched_files, task)

        if not files_to_cleanup:
            logger.info(f"  No files to clean up for task '{task.name}'")
            continue

        logger.info(
            f"  Found {len(matched_files)} files matching patterns,{len(files_to_cleanup)} files to clean up "
            f"(files_older_than={task.files_older_than}, keep_latest_only={task.keep_latest_version_only})"
        )

        for file in files_to_cleanup:
            if operations_performed >= max_file_operations:
                logger.info(
                    f"Reached max file operations limit ({max_file_operations}). "
                    "Stopping cleanup."
                )
                exiting_early = True
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
                assert task.archive_folder is not None
                datastore_manager.archive_file(
                    file,
                    task.archive_folder,
                )
                logger.info(f"  Archived: {file.path}")
                total_archived += 1
            elif task.cleanup_mode == CleanupMode.DELETE:
                datastore_manager.delete_file(file)
                logger.info(f"  Deleted: {file.path}")
                total_deleted += 1
            else:
                raise NotImplementedError(f"Unknown cleanup mode: {task.cleanup_mode}")

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
