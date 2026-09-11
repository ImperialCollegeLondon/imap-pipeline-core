"""Prefect flow for deleting old rows from PostgreSQL database tables."""

import logging

from prefect import flow
from prefect.states import Completed

from imap_mag.config.AppSettings import AppSettings
from imap_mag.db.DeleteOldRows import delete_old_rows
from imap_mag.util import DatetimeProvider
from prefect_server.constants import PREFECT_CONSTANTS

logger = logging.getLogger(__name__)


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.DELETE_OLD_DATABASE_ROWS,
)
async def delete_old_database_rows_flow(
    dry_run: bool | None = None,
):
    """Delete rows from a PostgreSQL database table that are older than a threshold.

    Args:
        dry_run: If True, only count rows that would be deleted, without deleting
            them. If None, uses the value from configuration.
    """
    app_settings = AppSettings()  # type: ignore
    config = app_settings.database_delete_rows
    dry_run = dry_run if dry_run is not None else config.dry_run

    affected = delete_old_rows(app_settings, dry_run, DatetimeProvider())
    tables = [task.table for task in app_settings.database_delete_rows.tasks]

    action_word = "would be" if dry_run else "were"
    if affected > 0:
        return Completed(
            message=f"{affected} total row(s) {action_word} deleted from tables: '{tables}'."
        )
    else:
        return Completed(
            message=f"No rows to delete from tables: '{tables}'",
            name=PREFECT_CONSTANTS.SKIPPED_STATE_NAME,
        )
