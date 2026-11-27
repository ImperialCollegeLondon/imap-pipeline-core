import logging
import os
from typing import cast

from prefect_sqlalchemy import SqlAlchemyConnector

from imap_mag.config.AppSettings import AppSettings

logger = logging.getLogger(__name__)


def load_database_url(env_name_or_block_name: str) -> str | None:
    """Get a database URL from an environment variable or a Prefect block.

    The environment variable is checked first. Otherwise the name is taken to be a
    Prefect SqlAlchemyConnector block, which is loaded synchronously so this also
    works inside async flows.

    Args:
        env_name_or_block_name: Environment variable name or Prefect block name.

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

    return db_url


async def get_database_connectionstring(
    app_settings: AppSettings,
    db_env_name_or_block_name_or_block: str | SqlAlchemyConnector | None,
) -> str:
    """Get a database connection string from an environment variable or Prefect block.

    The URL is returned as configured: crump (0.9+) and Database both accept any
    PostgreSQL URL form, so no driver is forced here.

    Args:
        app_settings: settings; postgres_upload names the default env var or block.
        db_env_name_or_block_name_or_block: Environment variable name, Prefect block
            name, or SqlAlchemyConnector block. None uses the postgres_upload setting.

    Returns:
        Database connection string.
    """
    lookup = db_env_name_or_block_name_or_block
    if lookup is None:
        logger.info("Using database connection info from app settings")
        lookup = app_settings.postgres_upload.database_url_env_var_or_block_name

    if lookup is None:
        raise RuntimeError(
            "Database connection information not provided. Cannot upload."
        )

    if isinstance(lookup, SqlAlchemyConnector):
        db_url = lookup._rendered_url.render_as_string(False)
    else:
        db_url = load_database_url(lookup)

    if db_url is None:
        raise ValueError("Invalid database connection input")

    return db_url
