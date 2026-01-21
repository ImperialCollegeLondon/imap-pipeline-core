import abc
import typing
from pathlib import Path

from imap_mag.io.file.IFilePathHandler import IFilePathHandler

T = typing.TypeVar("T", bound="IFilePathHandler")


class IDatastoreFileManager(abc.ABC):
    """Interface for output managers."""

    @abc.abstractmethod
    def add_file(self, original_file: Path, path_handler: T) -> tuple[Path, T]:
        """Add file to output location."""
