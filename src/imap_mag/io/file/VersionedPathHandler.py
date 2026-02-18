from dataclasses import dataclass

from imap_mag.io.file.SequenceablePathHandler import SequenceablePathHandler


@dataclass
class VersionedPathHandler(SequenceablePathHandler):
    """
    Interface for versioned path handlers.

    This class defines the interface for all path handlers that support versioning.
    """

    version: int = 1

    def get_sequence(self) -> int:
        return self.version

    def set_sequence(self, sequence: int) -> None:
        if not self.supports_sequencing():
            raise ValueError("This path handler does not support sequencing.")

        self.version = sequence

    def increase_sequence(self) -> None:
        if not self.supports_sequencing():
            raise ValueError("This path handler does not support sequencing.")

        self.version += 1

    @staticmethod
    def get_sequence_variable_name() -> str:
        return "version"
