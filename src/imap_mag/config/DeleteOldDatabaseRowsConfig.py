from pydantic import Field

from imap_mag.config.CommandConfig import CommandConfig


class DeleteOldDatabaseRowsConfig(CommandConfig):
    """Configuration for deleting old rows from PostgreSQL database tables."""

    database_url_env_var_or_block_name: str = Field(
        default="DATABASE_URL",
        description="Environment variable name or Prefect block name containing PostgreSQL connection string",
    )
    dry_run: bool = Field(
        default=True,
        description="If True, only report rows that would be deleted without actually deleting them",
    )
