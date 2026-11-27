import logging
import os
from typing import cast

from prefect_sqlalchemy import SqlAlchemyConnector

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
