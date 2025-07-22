import abc
import logging
import re
import typing
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

T = typing.TypeVar("T", bound="IFilePathHandler")


@dataclass
class IFilePathHandler(abc.ABC):
    """
    Interface for path handlers.

    This class defines the interface for all file path handlers.
    The path handlers can be used to manage file I/O operations,
    including versioning, folder structure and file naming conventions.
    """

    sequence: int = 1

    @abc.abstractmethod
    def supports_sequencing(self) -> bool:
        """Denotes whether this path handler supports sequence indexes."""
        pass

    @abc.abstractmethod
    def get_folder_structure(self) -> str:
        pass

    @abc.abstractmethod
    def get_unsequenced_pattern(self) -> re.Pattern:
        """Get regex pattern for unsequenced files."""

    @abc.abstractmethod
    def get_filename(self) -> str:
        pass

    @classmethod
    @abc.abstractmethod
    def from_filename(cls: type[T], filename: str | Path) -> T | None:
        """Instantiate a path handler from a file name."""
        pass
