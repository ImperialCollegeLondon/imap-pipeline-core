from pathlib import Path

from pydantic import Field

from imap_mag.config.CommandConfig import CommandConfig


class PostgresUploadConfig(CommandConfig):
    """Configuration for uploading CSV and CDF files to PostgreSQL database using crump."""

    paths_to_match: list[str] = Field(
        default_factory=list,
        description="List of path patterns to match files for upload e.g. 'science/mag/l2/*.csv'",
    )
    crump_config_path: Path = Field(
        default=Path("crump_config.yml"),
        description="Path to crump YAML configuration file that maps files to database tables",
    )
    database_url_env_var_or_block_name: str = Field(
        default="DATABASE_URL",
        description="Environment variable name or Prefect block name containing PostgreSQL connection string",
    )
    enable_history: bool = Field(
        default=False,
        description="Enable history tracking in _crump_history table",
    )
    max_records_per_cdf: int | None = Field(
        default=None,
        description="Maximum number of records to extract per variable from CDF files (None = all records)",
    )
