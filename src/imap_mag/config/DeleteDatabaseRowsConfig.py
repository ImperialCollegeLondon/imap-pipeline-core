import re

from pydantic import BaseModel, Field, field_validator

from imap_mag.config.CommandConfig import CommandConfig
from prefect_server.durationUtils import parse_duration


class DeleteRowsTask(BaseModel):
    """Configuration for a single delete rows task."""

    name: str = Field(description="Name of this delete rows task for logging")
    table: str = Field(description="Table to delete rows from")
    datetime_column: str = Field(
        description="Name of the column indicating the datetime"
    )
    older_than: str = Field(
        default="7d",
        description="Files rows than this duration will be deleted. Supports "
        "formats like '30d' (days), '12h' (hours), '45m' (minutes), '60s' (seconds)",
    )

    @field_validator("table", "datetime_column", mode="after")
    @classmethod
    def conforms(cls, value: str, pat: str = r"^[a-zA-Z_][\-\._a-zA-Z0-9\$]*$") -> str:
        if re.fullmatch(pat, value) is None:
            raise ValueError(f"Invalid identifier {value}")
        return value

    @field_validator("older_than", mode="before")
    @classmethod
    def parseable_timedelta(cls, value: str) -> str:
        parse_duration(value)
        return value


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
