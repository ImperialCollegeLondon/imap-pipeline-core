from dataclasses import dataclass

from imap_mag.io.file.SequenceablePathHandler import SequenceablePathHandler


@dataclass
class VersionedPathHandler(SequenceablePathHandler):
    """
    Interface for versioned path handlers.

    This class defines the interface for all path handlers that support versioning.
    """

    version: int = 1
    version_set_pending: bool = False

    def get_sequence(self) -> int:
        return self.version

    def set_sequence(self, sequence: int) -> None:
        self.version = sequence
        self.version_set_pending = True

    def increase_sequence(self) -> None:
        self.version += 1
        self.version_set_pending = True

    @staticmethod
    def get_sequence_variable_name() -> str:
        return "version"
