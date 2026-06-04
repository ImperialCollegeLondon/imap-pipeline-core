"""Prefect flow for indexing existing datastore files into the database."""

import logging

from prefect import flow
from prefect.states import Completed

from imap_mag.config.AppSettings import AppSettings
from imap_mag.db import Database
from imap_mag.io.DBIndexedDatastoreFileManager import (
    DBIndexedDatastoreFileManager,
)
from imap_mag.io.FilePathHandlerSelector import FilePathHandlerSelector
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import try_get_prefect_logger

logger = logging.getLogger(__name__)


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.DATASTORE_INDEXER,
)
async def index_datastore_flow():
    """Index all files in the datastore into the database.

    Walks the datastore directory recursively and, for each file that has a
    recognised path handler, ensures a database record exists:

    - If the file is already active in the database it is skipped.
    - If the file is not in the database a new record is created.
    - If the file has a database record that was previously soft-deleted the
      deletion date is cleared so the record becomes active again.
    """
    logger = try_get_prefect_logger(__name__)

    app_settings = AppSettings()  # type: ignore
    db = Database()
    datastore_manager = DBIndexedDatastoreFileManager(
        database=db, settings=app_settings
    )

    datastore_path = app_settings.data_store

    total_indexed = 0
    total_skipped = 0
    total_restored = 0
    total_no_handler = 0

    logger.info(f"Indexing datastore at {datastore_path}")

    for file in sorted(datastore_path.rglob("*")):
        if not file.is_file():
            continue

        path_handler = FilePathHandlerSelector.find_by_path(
            file, throw_if_not_found=False
        )

        if path_handler is None:
            logger.debug(f"No path handler found for {file}. Skipping.")
            total_no_handler += 1
            continue

        result = datastore_manager.index_existing_file(file, path_handler)

        if result == "indexed":
            total_indexed += 1
        elif result == "skipped":
            total_skipped += 1
        elif result == "restored":
            total_restored += 1

    logger.info(
        f"Datastore indexing complete: {total_indexed} indexed, "
        f"{total_skipped} skipped, {total_restored} restored, "
        f"{total_no_handler} without a handler."
    )

    parts = []
    if total_indexed > 0:
        parts.append(f"{total_indexed} indexed")
    if total_restored > 0:
        parts.append(f"{total_restored} restored")
    if total_skipped > 0:
        parts.append(f"{total_skipped} skipped")

    if parts:
        return Completed(message="Files: " + ", ".join(parts))
    else:
        return Completed(
            message="No files to index 💤",
            name=PREFECT_CONSTANTS.SKIPPED_STATE_NAME,
        )
