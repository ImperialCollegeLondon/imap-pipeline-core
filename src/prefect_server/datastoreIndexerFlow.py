"""Prefect flow for indexing existing datastore files into the database."""

import logging

from prefect import flow
from prefect.states import Completed

from imap_mag.client.SDCDataAccess import SDCDataAccess
from imap_mag.config.AppSettings import AppSettings
from imap_mag.db import Database
from imap_mag.io.DBIndexedDatastoreFileManager import (
    DBIndexedDatastoreFileManager,
    IndexResult,
)
from imap_mag.io.file import SPICEPathHandler
from imap_mag.io.FilePathHandlerSelector import FilePathHandlerSelector
from imap_mag.util import CONSTANTS, Environment
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.prefectUtils import get_secret_or_env_var

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

    new_spice_files = []
    for file in sorted(datastore_path.rglob("*")):
        if not file.is_file():
            continue

        path_handler = FilePathHandlerSelector.find_by_path(
            file, throw_if_not_found=False
        )

        if path_handler is None:
            logger.warning(f"No path handler found for {file}. Skipping.")
            total_no_handler += 1
            continue

        result = datastore_manager.index_existing_file(file, path_handler)
        if result == IndexResult.INDEXED and type(path_handler) is SPICEPathHandler:
            new_spice_files.append((file, path_handler))

        if result == IndexResult.INDEXED:
            total_indexed += 1
        elif result == IndexResult.SKIPPED:
            total_skipped += 1
        elif result == IndexResult.RESTORED:
            logger.info(f"Restored file {file} in database by clearing deletion date.")
            total_restored += 1

    if new_spice_files:
        auth_code = await get_secret_or_env_var(
            PREFECT_CONSTANTS.POLL_SCIENCE.SDC_AUTH_CODE_SECRET_NAME,
            CONSTANTS.ENV_VAR_NAMES.SDC_AUTH_CODE,
            raise_if_missing=False,
        )
        work_folder = app_settings.setup_work_folder_for_command(
            app_settings.fetch_spice
        )

        with (
            Environment(CONSTANTS.ENV_VAR_NAMES.SDC_AUTH_CODE, auth_code)
            if auth_code
            else Environment()
        ):
            for file, path_handler in new_spice_files:
                # if this is a spice file then do our best to query the metadata from the SDC for it
                data_access = SDCDataAccess(
                    auth_code=app_settings.fetch_spice.api.auth_code,
                    data_dir=work_folder,
                    sdc_url=app_settings.fetch_spice.api.url_base,
                )
                try:
                    results = data_access.spice_query(
                        file_name=path_handler.filename,
                    )
                    logger.info(f"SDC API returned {len(results)} results")
                except Exception as e:
                    logger.warning(f"Failed to query SDC for {file} with error: {e}")
                    results = None

                if results and len(results) > 0:
                    path_handler.add_metadata(results[0])
                    logger.info(f"Added metadata for {file} from SDC")
                    logger.debug(f"Metadata for {file}: {path_handler.get_metadata()}")
                    datastore_manager.index_existing_file(file, path_handler)
                else:
                    logger.warning(f"Unable to add metadata for {file} from SDC")

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

    if total_indexed > 0 or total_restored > 0:
        return Completed(message="Files: " + ", ".join(parts))
    else:
        return Completed(
            message="No files to index 💤",
            name=PREFECT_CONSTANTS.SKIPPED_STATE_NAME,
        )
