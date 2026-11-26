import logging
import os
from enum import StrEnum
from typing import cast

from prefect_sqlalchemy import SqlAlchemyConnector

from imap_mag.config.AppSettings import AppSettings

logger = logging.getLogger(__name__)


class DatabaseDriver(StrEnum):
    Unchanged = ""
    Postgresql = "postgresql"
    PostgresqlPsychopg = "postgresql+psycopg"


def load_database_url(
    env_name_or_block_name: str,
    database_driver: DatabaseDriver = DatabaseDriver.Unchanged,
) -> str | None:
    """Get a database URL from an environment variable or a Prefect block.

    The environment variable is checked first. Otherwise the name is taken to be a
    Prefect SqlAlchemyConnector block, which is loaded synchronously so this also
    works inside async flows.

    Args:
        env_name_or_block_name: Environment variable name or Prefect block name.
        database_driver: Driver to put in the URL, or Unchanged to keep the one given.

    Returns:
        The database URL, or None if there is no such variable or block.
    """
    db_url = os.getenv(env_name_or_block_name)
    if db_url:
        logger.info(
            f"Using database connection string from environment variable {env_name_or_block_name}"
        )
    else:
        try:
            # _sync=True makes load() return the block, not a coroutine.
            connector_block = cast(
                SqlAlchemyConnector,
                SqlAlchemyConnector.load(env_name_or_block_name, _sync=True),
            )
        except ValueError:
            logger.info(
                f"{env_name_or_block_name} SqlAlchemyConnector block not found or empty"
            )
            return None

        logger.info(
            f"Using database connection string from Prefect SqlAlchemyConnector block {env_name_or_block_name}\n{connector_block._rendered_url.render_as_string(True)}"
        )
        db_url = connector_block._rendered_url.render_as_string(False)

    return _set_driver(db_url, database_driver)


async def get_database_connectionstring(
    app_settings: AppSettings,
    db_env_name_or_block_name_or_block: str | SqlAlchemyConnector | None,
    database_driver: DatabaseDriver = DatabaseDriver.Unchanged,
) -> str:
    """
    Get database connection string from environment variable or Prefect block.

    Args:
        db_env_name_or_block_name_or_block: Environment variable name, Prefect block name, or SqlAlchemyConnector block.
    Returns:
        Database connection string.
    """

    if db_env_name_or_block_name_or_block is None:
        logger.info("Using database connection info from app settings")
        db_url_lookup_key = (
            app_settings.postgres_upload.database_url_env_var_or_block_name
        )
    else:
        db_url_lookup_key = db_env_name_or_block_name_or_block

    if db_url_lookup_key is None:
        raise RuntimeError(
            "Database connection information not provided. Cannot upload."
        )

    db_url = None

    if isinstance(db_env_name_or_block_name_or_block, SqlAlchemyConnector):
        db_url = _set_driver(
            db_env_name_or_block_name_or_block._rendered_url.render_as_string(False),
            database_driver,
        )
    elif isinstance(db_env_name_or_block_name_or_block, str):
        db_url = load_database_url(db_env_name_or_block_name_or_block, database_driver)

    if db_url is None:
        raise ValueError("Invalid database connection input")

    return db_url


def _set_driver(db_url: str, database_driver: DatabaseDriver) -> str:
    # Note that crump expects/needs: postgresql://user:pass@host:port/dbname so whenever
    # crump is used, the postgress driver should be forced.
    if database_driver == DatabaseDriver.Unchanged:
        return db_url
    return database_driver + "://" + db_url.split("://")[-1]
