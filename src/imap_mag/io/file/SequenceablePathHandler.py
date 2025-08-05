import abc
import re
from dataclasses import dataclass
from enum import Enum
from typing import Literal, final, overload

from imap_mag.io.file.IFilePathHandler import IFilePathHandler


class UnsequencedStyle(Enum):
    Regex = "regex"
    SQL = "sql"


@dataclass
class SequenceablePathHandler(IFilePathHandler):
    """
    Interface for sequenceable path handlers.

    This class defines the interface for path handlers that support
    some sort of sequencing (e.g., versioning or part numbers).
    """

    @final
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

    @overload
    def get_unsequenced_pattern(
        self, style: Literal[UnsequencedStyle.Regex]
    ) -> re.Pattern:
        """Get regex pattern for unsequenced files."""
        pass

    @overload
    def get_unsequenced_pattern(self, style: Literal[UnsequencedStyle.SQL]) -> str:
        """Get SQL pattern for unsequenced files."""
        pass

    @abc.abstractmethod
    def get_unsequenced_pattern(
        self, style: UnsequencedStyle = UnsequencedStyle.Regex
    ) -> re.Pattern | str:
        pass

    @staticmethod
    @abc.abstractmethod
    def get_sequence_variable_name() -> str:
        """Get the name of the variable denoting a "sequence" in the class and patterns."""
        pass
