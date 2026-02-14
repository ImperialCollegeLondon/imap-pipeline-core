from datetime import UTC, datetime
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field, model_validator

from imap_mag.config.CommandConfig import CommandConfig
from imap_mag.util import DatetimeProvider
from prefect_server.durationUtils import parse_duration


class CleanupMode(str, Enum):
    """Mode for cleanup operations."""

    DELETE = "delete"
    ARCHIVE = "archive"


class CleanupTask(BaseModel):
    """Configuration for a single cleanup task."""

    name: str = Field(description="Name of this cleanup task for logging")
    paths_to_match: list[str] = Field(
        default_factory=list,
        description="List of path patterns to match files for cleanup e.g. 'science/mag/l2/*.cdf'",
    )
    files_older_than: str = Field(
        default="30d",
        description="Files older than this duration will be considered for cleanup. "
        "Supports formats like '30d' (days), '12h' (hours), '45m' (minutes), '60s' (seconds)",
    )
    keep_latest_version_only: bool = Field(
        default=True,
        description="If True, only remove files that are not the latest version for their type/date. "
        "If False, remove all files matching the pattern and age criteria.",
    )
    cleanup_mode: CleanupMode = Field(
        default=CleanupMode.DELETE,
        description="Whether to delete files or archive them",
    )
    archive_folder: Path | None = Field(
        default=None,
        description="Folder to archive files to. Required when cleanup_mode is 'archive'. "
        "The folder structure relative to datastore root will be preserved.",
    )

    @model_validator(mode="after")
    def validate_archive_folder(self) -> "CleanupTask":
        if self.cleanup_mode == CleanupMode.ARCHIVE and self.archive_folder is None:
            raise ValueError(
                "archive_folder is required when cleanup_mode is 'archive'"
            )
        return self

    def get_file_age_cutoff(self) -> datetime:
        """Get the age cutoff datetime based on current time and files_older_than."""

        duration = parse_duration(self.files_older_than)
        cutoff = DatetimeProvider.now() - duration
        return cutoff.replace(tzinfo=UTC)


class DatastoreCleanupConfig(CommandConfig):
    """Configuration for cleaning up files from the datastore."""

    tasks: list[CleanupTask] = Field(
        default_factory=list,
        description="List of cleanup tasks to run",
    )
    dry_run: bool = Field(
        default=True,
        description="If True, only log files that would be removed/moved without actually doing it",
    )
