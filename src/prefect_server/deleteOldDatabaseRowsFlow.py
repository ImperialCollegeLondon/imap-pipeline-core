"""Prefect flow for deleting old rows from PostgreSQL database tables."""

import logging

from prefect import flow
from prefect.states import Completed
from prefect_sqlalchemy import SqlAlchemyConnector

from imap_mag.config.AppSettings import AppSettings
from imap_mag.db.DeleteOldRows import delete_old_rows
from imap_mag.util import DatetimeProvider
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.postgresUploadFlow import _get_database_connectionstring
from prefect_server.prefectUtils import try_get_prefect_logger

logger = logging.getLogger(__name__)


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.DELETE_OLD_DATABASE_ROWS,
)
async def delete_old_database_rows_flow(
    table: str,
    older_than_days: int = 7,
    datetime_column: str = "id",
    dry_run: bool | None = None,
    db_env_name_or_block_name_or_block: str
    | SqlAlchemyConnector
    | None = PREFECT_CONSTANTS.IMAP_DATABASE_BLOCK_NAME,
):
    """
    Delete rows from a PostgreSQL database table that are older than a threshold.

    By default, this can be used to delete rows older than 7 days from the NOAA
    tables (ace_mag_noaa, ace_wind_noaa, solar_mag_noaa, solar_wind_noaa), which
    all use "id" as their datetime column.

    Args:
        table: Name of the database table to delete rows from.
        older_than_days: Rows where datetime_column is older than this many days
            will be deleted.
        datetime_column: Name of the datetime column used to determine the age
            of a row.
        dry_run: If True, only count rows that would be deleted, without deleting
            them. If None, uses the value from configuration.
        db_env_name_or_block_name_or_block: Environment variable name, Prefect
            block name, or SqlAlchemyConnector block used to connect to the
            database. If None, uses the value configured for this task's settings.
    """
    logger = try_get_prefect_logger(__name__)

    app_settings = AppSettings()  # type: ignore
    config = app_settings.delete_old_database_rows
    dry_run = dry_run if dry_run is not None else config.dry_run

    db_env_name_or_block_name_or_block = (
        db_env_name_or_block_name_or_block or config.database_url_env_var_or_block_name
    )
    db_url = await _get_database_connectionstring(
        app_settings, db_env_name_or_block_name_or_block
    )

    logger.info(
        f"Deleting rows from '{table}' where {datetime_column} is older than "
        f"{older_than_days} day(s). Dry run: {dry_run}"
    )

    affected = delete_old_rows(
        db_url, table, datetime_column, older_than_days, dry_run, DatetimeProvider()
    )

    action_word = "would be" if dry_run else "were"
    if affected > 0:
        return Completed(
            message=f"{affected} row(s) {action_word} deleted from '{table}'"
        )
    else:
        return Completed(
            message=f"No rows to delete from '{table}' 💤",
            name=PREFECT_CONSTANTS.SKIPPED_STATE_NAME,
        )
