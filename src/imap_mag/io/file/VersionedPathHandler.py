from dataclasses import dataclass
from enum import Enum

from imap_mag.io.file.SequenceablePathHandler import SequenceablePathHandler


@dataclass
class VersionedPathHandler(SequenceablePathHandler):
    """
    Interface for versioned path handlers.

    This class defines the interface for all path handlers that support versioning.
    """

    class VersionMode(Enum):
        MAX_VERSION_PLUS_ONE = "auto"
        USER_OVERRIDE = "user_override"

    version: int = 1
    version_major: int = 0
    # When True the datastore managers skip version up-versioning and overwrite any
    # existing file at this exact version. Only set for the user version override; the
    # normal operating default keeps auto-versioning and overwrite protection.
    versioning_mode: VersionMode = VersionMode.MAX_VERSION_PLUS_ONE

    def supports_sequencing(self) -> bool:
        """Denotes whether this path handler supports sequence-like indexes."""
        return (
            self.versioning_mode
            == VersionedPathHandler.VersionMode.MAX_VERSION_PLUS_ONE
        )

    def get_sequence(self) -> int:
        return self.version

    def can_change_sequence(self) -> bool:
        return self.supports_sequencing()

    def set_sequence(self, sequence: int) -> None:
        if not self.supports_sequencing():
            raise ValueError("This file does not support changing version/sequence. ")

        self.version = sequence

    def increase_sequence(self) -> None:
        if not self.supports_sequencing():
            raise ValueError("This file does not support changing version/sequence. ")

        self.version += 1

    @staticmethod
    def get_sequence_variable_name() -> str:
        return "version"
