import abc
import typing
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from typing_extensions import deprecated

from imap_mag.io.file.IFilePathHandler import IFilePathHandler

T = typing.TypeVar("T", bound="IFilePathHandler")


class DatastoreFileAction(StrEnum):
    """What happened to the datastore when a file was stored."""

    FILE_CREATED = "file_created"
    """The file did not exist in the datastore and was newly created."""

    UPDATE_SKIPPED_IDENTICAL = "update_skipped_identical"
    """An identical file already existed in the datastore so it was left untouched."""

    UPDATE_OVERWRITTEN = "update_overwritten"
    """A file with different content already existed in the datastore and was overwritten."""


@dataclass(frozen=True)
class DatastoreStoreResult(typing.Generic[T]):
    """Summary of storing a file in the datastore."""

    destination: Path
    """The path of the file in the datastore."""

    path_handler: T
    """The path handler used to generate the destination, including any version changes."""

    action: DatastoreFileAction
    """What happened to the datastore."""


class IDatastoreFileManager(abc.ABC):
    """Interface for output managers."""

    @abc.abstractmethod
    def store_file(
        self, original_file: Path, path_handler: T
    ) -> DatastoreStoreResult[T]:
        """Store a file in the datastore.

        Args:
            original_file: The file to store.
            path_handler: The path handler describing where the file belongs.

        Returns:
            A summary of the destination file, the path handler used to generate it
            (which may have had its version changed) and the action performed.
        """

    @deprecated("Use store_file, which reports the action performed, instead.")
    def add_file(self, original_file: Path, path_handler: T) -> tuple[Path, T, bool]:
        """Add file to output location.

        Returns the destination file path, the path handler used to generate it, and a bool indicating if an overwrite occurred.
        """
        result = self.store_file(original_file, path_handler)
        return (
            result.destination,
            result.path_handler,
            result.action == DatastoreFileAction.UPDATE_OVERWRITTEN,
        )
