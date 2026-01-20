import fnmatch
import logging
from datetime import UTC, datetime
from pathlib import Path

import prefect_managedfiletransfer
from prefect import State, flow
from prefect.filesystems import LocalFileSystem
from prefect.states import Completed
from prefect_managedfiletransfer import (
    RCloneConfigFileBlock,
    ServerWithBasicAuthBlock,
    ServerWithPublicKeyAuthBlock,
)

from imap_mag.config.AppSettings import AppSettings
from imap_mag.db import Database
from imap_mag.util.constants import CONSTANTS
from prefect_server.constants import PREFECT_CONSTANTS

logger = logging.getLogger(__name__)


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.SHAREPOINT_UPLOAD,
    log_prints=True,
)
async def upload_shared_docs_flow(
    destination_block_or_blockname: (
        ServerWithBasicAuthBlock
        | ServerWithPublicKeyAuthBlock
        | LocalFileSystem
        | RCloneConfigFileBlock
        | str
    ) = PREFECT_CONSTANTS.DEFAULT_UPLOAD_DESTINATION_BLOCK_NAME,
    find_files_after: datetime | None = None,
    how_many: int | None = None,
    do_uploads: bool = True,
    do_deletes: bool = True,
    workflow_progress_key: str = "sharepoint-upload",
):
    """
    Publish new files to sharepoint/box/whatever configured cloud storage
    """

    app_settings = AppSettings()  # type: ignore
    db = Database()
    started = datetime.now(tz=UTC)

    uploaded_files = (
        await upload_new_files(
            destination_block_or_blockname,
            how_many,
            app_settings,
            db,
            started,
            find_files_after,
            workflow_progress_key,
        )
        if do_uploads
        else 0
    )

    deleted_files = (
        await remove_deleted_files(
            destination_block_or_blockname,
            how_many,
            app_settings,
            db,
            started,
            find_files_after,
            workflow_progress_key,
        )
        if do_deletes
        else 0
    )

    result: State
    if uploaded_files or deleted_files:
        result = Completed(
            message=f"{uploaded_files} files uploaded, {deleted_files} files deleted"
        )
    else:
        result = Completed(
            message="No work to do 💤", name=PREFECT_CONSTANTS.SKIPPED_STATE_NAME
        )

    return result


async def upload_new_files(
    destination_block_or_blockname,
    how_many: int | None,
    app_settings: AppSettings,
    db: Database,
    started: datetime,
    find_files_after: datetime | None,
    workflow_progress_key: str,
) -> int:
    workflow_progress = db.get_workflow_progress(workflow_progress_key)
    if workflow_progress.progress_timestamp is None:
        workflow_progress.progress_timestamp = CONSTANTS.IMAP_EPOCH_DATETIME

    last_modified_date = (
        workflow_progress.progress_timestamp
        if find_files_after is None
        else find_files_after
    )

    logger.info(
        f"Looking for {how_many if how_many else 'all'} files modified after {last_modified_date}"
    )

    new_files_db = db.get_files_since(last_modified_date, how_many)

    workflow_progress.update_last_checked_date(started)

    logger.info(
        f"Found {len(new_files_db)} new files. Checking against {len(app_settings.upload.paths_to_match)} patterns from settings."
    )

    files = [
        f
        for f in new_files_db
        if any(fnmatch.fnmatch(f.path, p) for p in app_settings.upload.paths_to_match)
    ]

    logger.info(
        f"{len(files)} files matching upload patterns.\n Publishing {', '.join(str(f) for f in files)}"
    )

    sharepoint_root = Path(app_settings.upload.root_path)

    for file in files:
        path_inside_datastore = Path(file.path)
        if app_settings.data_store in Path(file.path).parents:
            path_inside_datastore = path_inside_datastore.absolute().relative_to(
                app_settings.data_store.absolute()
            )
        path_inc_datastore = app_settings.data_store / path_inside_datastore
        destination_path = sharepoint_root / path_inside_datastore

        if not path_inc_datastore.exists():
            logger.warning(
                f"File {path_inside_datastore} does not exist, skipping upload."
            )
            continue

        await prefect_managedfiletransfer.upload_file_flow(
            destination_block_or_blockname=destination_block_or_blockname,
            source_folder=path_inc_datastore.parent,
            pattern_to_upload=path_inc_datastore.name,
            destination_file=destination_path,
            update_only_if_newer_mode=True,
            mode=prefect_managedfiletransfer.TransferType.Copy,
        )

    if files:
        logger.debug("Uploading completed")
        latest_file_timestamp = max(f.last_modified_date for f in files)
        new_progress_date = min(started, latest_file_timestamp.astimezone(UTC))
        workflow_progress.update_progress_timestamp(new_progress_date)
        logger.info(
            f"Set progress timestamp for {workflow_progress_key} to {new_progress_date}"
        )

    db.save(workflow_progress)
    logger.info(f"{len(files)} file(s) uploaded")
    return len(files)


async def remove_deleted_files(
    destination_block_or_blockname,
    how_many: int | None,
    app_settings: AppSettings,
    db: Database,
    started: datetime,
    find_files_after: datetime | None,
    workflow_progress_key: str,
) -> int:
    workflow_progress = db.get_workflow_progress(workflow_progress_key + "-deletes")
    if workflow_progress.progress_timestamp is None:
        workflow_progress.progress_timestamp = CONSTANTS.IMAP_EPOCH_DATETIME

    last_modified_date = (
        workflow_progress.progress_timestamp
        if find_files_after is None
        else find_files_after
    )

    logger.info(
        f"Looking for {how_many if how_many else 'all'} files deleted after {last_modified_date}"
    )

    # TODO: Implement this and the finish this method like the upload flow, but with deletes
    # deleted_files_db = db.get_files_deleted_since(last_modified_date, how_many)
    return 0
