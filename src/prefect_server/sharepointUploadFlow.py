import fnmatch
from datetime import datetime, timezone
from pathlib import Path

import prefect_managedfiletransfer
from prefect import flow, get_run_logger
from prefect.states import Completed

from imap_mag.config.AppSettings import AppSettings
from imap_mag.db import Database
from prefect_server.constants import PREFECT_CONSTANTS

PROGRESS_KEY = "sharepoint-upload"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.SHAREPOINT_UPLOAD,
    log_prints=True,
)
async def upload_new_files_to_sharepoint(
    find_files_after: datetime | None = None, how_many: int | None = None
):
    """
    Publish new files to sharepoint
    """

    logger = get_run_logger()

    app_settings = AppSettings()  # type: ignore
    db = Database()
    started = datetime.now(tz=timezone.utc)
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

    new_files_db = db.get_files_since(last_modified_date, how_many)

    workflow_progress.record_checked_download(started)

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
            destination_block_or_blockname=PREFECT_CONSTANTS.SHAREPOINT_BLOCK_NAME,
            source_folder=path_inc_datastore.parent,
            pattern_to_upload=path_inc_datastore.name,
            destination_file=destination_path,
            update_only_if_newer_mode=True,
            mode=prefect_managedfiletransfer.TransferType.Copy,
        )

    result = None
    if files:
        logger.debug("Uploading completed")
        latest_file_timestamp = max(f.last_modified_date for f in files)
        new_progress_date = min(started, latest_file_timestamp.astimezone(timezone.utc))
        workflow_progress.record_successful_download(new_progress_date)
        logger.info(f"Set progress timestamp for {PROGRESS_KEY} to {new_progress_date}")

        result = Completed(message=f"{len(files)} files uploaded")
    else:
        result = Completed(message="No work to do 💤", name="Skipped")

    db.save(workflow_progress)
    logger.info(f"{len(files)} file(s) uploaded to SharePoint")
    return result
