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

    version: int = 0

    @abc.abstractmethod
    def supports_versioning(self) -> bool:
        """Denotes whether this path handler supports versioning."""

    @abc.abstractmethod
    def get_folder_structure(self) -> str:
        pass

    @abc.abstractmethod
    def get_unversioned_pattern(self) -> re.Pattern:
        """Get regex pattern for unversioned files."""

    @abc.abstractmethod
    def get_filename(self) -> str:
        pass

    @classmethod
    @abc.abstractmethod
    def from_filename(cls: type[T], filename: str | Path) -> T | None:
        """Instantiate a path handler from a file name."""
