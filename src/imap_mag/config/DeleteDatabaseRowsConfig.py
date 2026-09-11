from pydantic import BaseModel, Field

from imap_mag.config.CommandConfig import CommandConfig


class DeleteRowsTask(BaseModel):
    """Configuration for a single delete rows task."""

    name: str = Field(description="Name of this delete rows task for logging")
    table: str = Field(description="Table to delete rows from")
    datetime_column: str = Field(
        description="Name of the column indicating the datetime"
    )
    threshold_days: int = Field(
        description="Delete rows older than this value, in days"
    )


class DeleteDatabaseRowsConfig(CommandConfig):
    """Configuration for deleting rows from the datastore."""

    tasks: list[DeleteRowsTask] = Field(
        default_factory=list,
        description="List of delete rows tasks to run",
    )
    dry_run: bool = Field(
        default=True,
        description="If True, only log number of rows that would be deleted without actually doing it",
    )
    database_url_env_var_or_block_name: str = Field(
        default="DATABASE_URL",
        description="Environment variable name or Prefect block name containing PostgreSQL connection string",
    )
