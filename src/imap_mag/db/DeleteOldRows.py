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

    # `get_database_connectionstring` strips the psycopg driver suffix so the URL
    # can be handed to crump (used by the postgres-upload flow), which expects a bare
    # "postgresql://" scheme. This module talks to the database directly through
    # SQLAlchemy/`Database`, though, which needs the driver specified explicitly -
    # otherwise SQLAlchemy defaults to psycopg2, which isn't installed in this project.
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)

    db = Database(db_url)

    total_deleted = 0
    for task in config.tasks:
        try:
            cutoff = datetime_provider.now() - timedelta(days=task.threshold_days)
            # Each task gets its own transaction. Postgres aborts the entire
            # transaction as soon as one statement fails (e.g. a missing table),
            # so sharing a single transaction across tasks would cause every
            # subsequent task to fail with "current transaction is aborted",
            # even though we've already handled/logged the earlier failure.
            with db.engine.begin() as connection:
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
                raise
    return total_deleted
