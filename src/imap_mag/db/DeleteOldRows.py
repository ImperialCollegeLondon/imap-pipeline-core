"""Utilities for deleting old rows from PostgreSQL database tables."""

import logging
from datetime import timedelta

from psycopg.errors import UndefinedTable
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from imap_mag.config.AppSettings import AppSettings
from imap_mag.db import Database
from imap_mag.db.utils import get_database_connectionstring
from imap_mag.util import DatetimeProvider

logger = logging.getLogger(__name__)


async def delete_old_rows(
    app_settings: AppSettings,
    dry_run: bool | None = None,
    datetime_provider: DatetimeProvider = DatetimeProvider(),
) -> int:
    """Delete rows from db table where the datetime column is older than a threshold.

    Args:
        app_settings: Common settings and specific task configuration.
        dry_run: If True, only count matching rows without deleting them.
        datetime_provider: Provider for the current time, used to compute the age cutoff.

    Returns:
        Number of rows deleted (or that would be deleted, in dry-run mode).
    """
    config = app_settings.database_delete_rows
    dry_run = dry_run if dry_run is not None else config.dry_run
    db_url = await get_database_connectionstring(
        app_settings, config.database_url_env_var_or_block_name
    )
    db = Database(db_url)

    total_deleted = 0
    with db.engine.begin() as connection:
        for task in config.tasks:
            try:
                cutoff = datetime_provider.now() - timedelta(days=task.threshold_days)
                if dry_run:
                    statement = text(
                        f'SELECT COUNT(*) FROM "{task.table}" WHERE "{task.datetime_column}" < :cutoff'
                    )
                    count = connection.execute(
                        statement, {"cutoff": cutoff}
                    ).scalar_one()
                    logger.info(
                        f"[DRY RUN] Would delete {count} row(s) from '{task.table}' "
                        f"where {task.datetime_column} < {cutoff.isoformat()}"
                    )
                    total_deleted += int(count)
                else:
                    statement = text(
                        f'DELETE FROM "{task.table}" WHERE "{task.datetime_column}" < :cutoff'
                    )
                    result = connection.execute(statement, {"cutoff": cutoff})
                    logger.info(
                        f"Deleted {result.rowcount} row(s) from '{task.table}' "
                        f"where {task.datetime_column} < {cutoff.isoformat()}"
                    )
                    total_deleted += result.rowcount

            except ProgrammingError as e:
                if isinstance(e.orig, UndefinedTable):
                    logger.warning(
                        f"Table '{task.table}' does not exist - nothing to delete"
                    )
                else:
                    db.engine.dispose()
                    raise
    return total_deleted
