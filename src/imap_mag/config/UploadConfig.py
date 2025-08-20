from pydantic import Field

from imap_mag.config.CommandConfig import CommandConfig


class UploadConfig(CommandConfig):
    paths_to_match: list[str] = Field(
        default_factory=list,
        description="List of path patterns to upload e.g. 'science/mag/l2'",
    )
    root_path: str = Field(
        default="Flight Data (dev)",
        description="Root path in destination/SharePoint for uploads",
    )
