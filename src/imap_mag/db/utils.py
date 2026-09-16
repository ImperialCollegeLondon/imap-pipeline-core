import logging
import os

from prefect_sqlalchemy import SqlAlchemyConnector

from imap_mag.config.AppSettings import AppSettings

logger = logging.getLogger(__name__)


async def get_database_connectionstring(
    app_settings: AppSettings,
    db_env_name_or_block_name_or_block: str | SqlAlchemyConnector | None,
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
        db_url = db_env_name_or_block_name_or_block._rendered_url.render_as_string(
            False
        )
    elif isinstance(db_env_name_or_block_name_or_block, str):
        # Check if it's an environment variable
        env_value = os.getenv(db_env_name_or_block_name_or_block)
        if env_value:
            logger.info(
                f"Using database connection string from environment variable {db_env_name_or_block_name_or_block}"
            )
            db_url = env_value
        else:
            # Assume it's a Prefect block name
            try:
                connector_block = await SqlAlchemyConnector.aload(
                    db_env_name_or_block_name_or_block
                )
                logger.info(
                    f"Using database connection string from Prefect SqlAlchemyConnector block {db_env_name_or_block_name_or_block}\n{connector_block._rendered_url.render_as_string(True)}"
                )
                db_url = connector_block._rendered_url.render_as_string(False)
            except ValueError:
                logger.info(
                    f"{db_env_name_or_block_name_or_block} SqlAlchemyConnector block not found or empty"
                )

    if db_url is None:
        raise ValueError("Invalid database connection input")

    # Convert PostgreSQL URL for crump (remove +psycopg driver specification)
    # crump expects/needs: postgresql://user:pass@host:port/dbname
    db_url = db_url.replace("postgresql+psycopg://", "postgresql://")

    return db_url
