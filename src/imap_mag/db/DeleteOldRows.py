"""Utilities for deleting old rows from PostgreSQL database tables."""

import logging
import re
from datetime import timedelta

from psycopg.errors import UndefinedTable
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

from imap_mag.util import DatetimeProvider

logger = logging.getLogger(__name__)

_VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, kind: str) -> None:
    """Ensure a table/column name is a safe SQL identifier before interpolating it.

    Args:
        name: The identifier to validate.
        kind: Human-readable description of the identifier, used in error messages.

    Raises:
        ValueError: If the identifier is not a simple alphanumeric/underscore name.
    """
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(f"Invalid {kind} name: {name!r}")


def delete_old_rows(
    db_url: str,
    table: str,
    datetime_column: str,
    older_than_days: float,
    dry_run: bool,
    datetime_provider: DatetimeProvider = DatetimeProvider(),
) -> int:
    """Delete rows from a database table where the datetime column is older than a threshold.

    Tables managed by crump (e.g. the NOAA tables) are only created once data has
    actually been synced into them by the postgres-upload flow. If the target table
    does not exist yet, this is treated as "nothing to delete" rather than an error.

    Args:
        db_url: SQLAlchemy-compatible PostgreSQL connection string.
        table: Name of the database table to delete rows from.
        datetime_column: Name of the datetime column used to determine the age of a row.
        older_than_days: Rows where datetime_column is older than this many days will be deleted.
        dry_run: If True, only count matching rows without deleting them.
        datetime_provider: Provider for the current time, used to compute the age cutoff.

    Returns:
        Number of rows deleted (or that would be deleted, in dry-run mode). Returns 0
        if the target table does not exist.
    """
    _validate_identifier(table, "table")
    _validate_identifier(datetime_column, "column")

    cutoff = datetime_provider.now() - timedelta(days=older_than_days)

    # Ensure we use the psycopg (v3) driver, which is the one available in this project.
    if db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)

    engine = create_engine(db_url)
    try:
        with engine.begin() as connection:
            if dry_run:
                statement = text(
                    f'SELECT COUNT(*) FROM "{table}" WHERE "{datetime_column}" < :cutoff'
                )
                count = connection.execute(statement, {"cutoff": cutoff}).scalar_one()
                logger.info(
                    f"[DRY RUN] Would delete {count} row(s) from '{table}' "
                    f"where {datetime_column} < {cutoff.isoformat()}"
                )
                return int(count)

            statement = text(
                f'DELETE FROM "{table}" WHERE "{datetime_column}" < :cutoff'
            )
            result = connection.execute(statement, {"cutoff": cutoff})
            logger.info(
                f"Deleted {result.rowcount} row(s) from '{table}' "
                f"where {datetime_column} < {cutoff.isoformat()}"
            )
            return result.rowcount
    except ProgrammingError as e:
        if isinstance(e.orig, UndefinedTable):
            logger.warning(
                f"Table '{table}' does not exist yet (it is created on first "
                "sync by the postgres-upload flow); treating as 0 rows to delete."
            )
            return 0
        raise
    finally:
        engine.dispose()
