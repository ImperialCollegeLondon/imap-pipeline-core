import abc
import re
from dataclasses import dataclass

from imap_mag.io.file.IFilePathHandler import IFilePathHandler


@dataclass
class SequenceablePathHandler(IFilePathHandler):
    """
    Interface for sequenceable path handlers.

    This class defines the interface for path handlers that support
    some sort of sequencing (e.g., versioning or part numbers).
    """

    def supports_sequencing(self) -> bool:
        """Denotes whether this path handler supports sequence-like indexes."""
        return True

    @abc.abstractmethod
    def get_sequence(self) -> int:
        """Retrieve the sequence count."""
        pass

    @abc.abstractmethod
    def set_sequence(self, sequence: int) -> None:
        """Set the sequence count."""
        pass

    @abc.abstractmethod
    def increase_sequence(self) -> None:
        """Increase the sequence count by 1."""
        pass

    @abc.abstractmethod
    def get_unsequenced_pattern(self) -> re.Pattern:
        """Get regex pattern for unsequenced files."""
        pass

    @abc.abstractmethod
    def get_sequence_variable_name(self) -> str:
        """Get the name of the variable denoting a "sequence" in the class and patterns."""
        pass
