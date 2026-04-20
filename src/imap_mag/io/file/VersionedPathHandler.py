from dataclasses import dataclass

from imap_mag.io.file.SequenceablePathHandler import SequenceablePathHandler


@dataclass
class VersionedPathHandler(SequenceablePathHandler):
    """
    Interface for versioned path handlers.

    This class defines the interface for all path handlers that support versioning.
    """

    version: int = 1
    _version_is_locked: bool = False

    def get_sequence(self) -> int:
        return self.version

    def lock_version(self) -> None:
        self._version_is_locked = True

    def set_sequence(self, sequence: int) -> None:
        if self._version_is_locked:
            raise ValueError("Version is locked and cannot be changed.")

        if not self.supports_sequencing():
            raise ValueError("This path handler does not support sequencing.")

        self.version = sequence

    def increase_sequence(self) -> None:
        if self._version_is_locked:
            raise ValueError("Version is locked and cannot be changed.")

        if not self.supports_sequencing():
            raise ValueError("This path handler does not support sequencing.")

        self.version += 1

    def supports_sequencing(self) -> bool:

        if self._version_is_locked:
            return False

        return super().supports_sequencing()

    @staticmethod
    def get_sequence_variable_name() -> str:
        return "version"
