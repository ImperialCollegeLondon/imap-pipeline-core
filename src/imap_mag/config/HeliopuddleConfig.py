from datetime import timedelta

from pydantic import AliasChoices, Field, field_validator, model_validator

from imap_mag.config.CommandConfig import CommandConfig


def _parse_duration(value: str | int | float | timedelta) -> timedelta:
    if isinstance(value, timedelta):
        return value
    if isinstance(value, int | float):
        return timedelta(seconds=value)

    from prefect_server.durationUtils import parse_duration

    return parse_duration(value)


class HeliopuddleConfig(CommandConfig):
    """Optional mirroring of the IMAP files table into a heliopuddle project.

    When enabled, every insert, update and soft delete of a row in the ``files``
    table is repeated in heliopuddle through its client and API as soon as the
    IMAP database transaction commits, and ``imap-mag heliopuddle sync`` (or the
    heliopuddle-sync flow) migrates or reconciles all files in bulk.

    Credentials are read from the environment, e.g. MAG_HELIOPUDDLE_USERNAME and
    MAG_HELIOPUDDLE_PASSWORD, MAG_HELIOPUDDLE_TOKEN, or MAG_HELIOPUDDLE_CLIENT_ID
    and MAG_HELIOPUDDLE_CLIENT_SECRET.
    """

    enabled: bool = False
    # API base URL including /api/v1, e.g. http://host:8080/api/v1
    url: str | None = None
    project: str | None = None
    # Storage location in the heliopuddle project; None means the project's first.
    storage_location: str | None = None
    # True when the heliopuddle storage location is the IMAP data store itself, so
    # files are registered in place. False uploads each file's content.
    shared_datastore: bool = True
    # Raise an error from the database write when heliopuddle could not be updated.
    # The IMAP database change is already committed; the next sync repairs it.
    fail_on_error: bool = True
    request_timeout: timedelta = timedelta(minutes=5)
    # How many times to retry a file that another heliopuddle operation owns.
    busy_retries: int = Field(default=3, ge=0)
    # Incremental syncs also re-check files modified this long before the last
    # sync, to allow for clock differences between writers.
    incremental_overlap: timedelta = timedelta(hours=1)
    # Sent in the user header when heliopuddle runs in header auth mode.
    user: str | None = "imap-mag"

    username: str | None = None
    password: str | None = None
    token: str | None = None
    # Fields with an underscore in their name cannot be read from MAG_HELIOPUDDLE_*
    # env vars by the nested settings parser, so they also take an explicit alias.
    client_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("client_id", "MAG_HELIOPUDDLE_CLIENT_ID"),
    )
    client_secret: str | None = Field(
        default=None,
        validation_alias=AliasChoices("client_secret", "MAG_HELIOPUDDLE_CLIENT_SECRET"),
    )

    work_sub_folder: str | None = "heliopuddle_sync"

    @field_validator("request_timeout", "incremental_overlap", mode="before")
    @classmethod
    def _duration(cls, value: str | int | float | timedelta) -> timedelta:
        return _parse_duration(value)

    @model_validator(mode="after")
    def _required_when_enabled(self) -> "HeliopuddleConfig":
        if self.enabled:
            missing = [name for name in ("url", "project") if not getattr(self, name)]
            if missing:
                raise ValueError(
                    f"heliopuddle integration is enabled but {', '.join(missing)} is not set"
                )
        return self
