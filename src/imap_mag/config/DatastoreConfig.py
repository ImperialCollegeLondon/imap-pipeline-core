from pydantic import BaseModel, Field


class DatastoreConfig(BaseModel):
    """Configuration for datastore behaviour."""

    disk_usage_threshold: float = Field(
        default=0.95,
        ge=0.0,
        le=1.0,
        description="Maximum disk usage fraction (0.0-1.0) before file delivery to the datastore "
        "is blocked. Default 0.95 means delivery is blocked when the filesystem is 95 % or more full.",
    )
