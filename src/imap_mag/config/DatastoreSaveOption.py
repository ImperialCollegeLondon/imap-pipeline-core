from enum import StrEnum


class DatastoreSaveOption(StrEnum):
    """Controls overwrite behaviour when saving files to the datastore."""

    FILE_OVERWRITES_BLOCKED = "FILE_OVERWRITES_BLOCKED"
    FILE_OVERWRITES_ALLOWED = "FILE_OVERWRITES_ALLOWED"
