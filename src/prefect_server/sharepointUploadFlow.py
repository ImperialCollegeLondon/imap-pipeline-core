from datetime import datetime, timezone
from pathlib import Path

import prefect_managedfiletransfer
from prefect import flow, get_run_logger

from imap_db.model import File
from imap_mag.db import Database
from prefect_server.constants import CONSTANTS

PROGRESS_KEY = "sharepoint-upload"


@flow(
    name=CONSTANTS.FLOW_NAMES.SHAREPOINT_UPLOAD,
    log_prints=True,
)
async def upload_new_files_to_sharepoint():
    """
    Publish new files to sharepoint
    """

    logger = get_run_logger()

    db = Database()
    started = datetime.now(tz=timezone.utc)
    download_progress = db.get_download_progress(PROGRESS_KEY)
    if download_progress.progress_timestamp is None:
        download_progress.progress_timestamp = datetime(2010, 1, 1)
    new_files_db = db.get_files(File.date > download_progress.progress_timestamp)

    path_patterns_to_upload = [
        "science/mag/l1b",
        "science/mag/l1c",
        "science/mag/l1d",
        "science/mag/l2",
        "science/mag/l2-pre",
        "hk/mag/l1",
    ]

    files = [
        Path(f.path)
        for f in new_files_db
        if any(p in f.path for p in path_patterns_to_upload)
    ]

    logger.info(f"Publishing {len(files)} files: {', '.join(str(f) for f in files)}")

    data_root_to_remove = Path("/data")
    sharepoint_root = Path("Flight Data")

    for file in files:
        destination_without_root = sharepoint_root / file.relative_to(
            data_root_to_remove
        )

        await prefect_managedfiletransfer.upload_file_flow(
            destination_block_or_blockname=CONSTANTS.SHAREPOINT_BLOCK_NAME,
            source_folder=file.parent,
            pattern_to_upload=file.name,
            destination_file=destination_without_root,
            update_only_if_newer_mode=True,
            mode=prefect_managedfiletransfer.TransferType.Copy,
        )

    download_progress.record_checked_download(started)
    if new_files_db:
        latest_timestamp = max(f.date for f in new_files_db)
        logger.debug(f"Latest downloaded timestamp for files is {latest_timestamp}.")
        download_progress.record_successful_download(latest_timestamp)
    else:
        logger.info("No new files to upload.")

    db.save(download_progress)
    logger.info("All files uploaded to SharePoint.")
