import abc
import hashlib
import logging
import typing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

T = typing.TypeVar("T", bound="IFilePathHandler")


@dataclass
class IFilePathHandler(abc.ABC):
    """
    Interface for path handlers.

    This class defines the interface for all file path handlers.
    The path handlers can be used to manage file I/O operations,
    including folder structure and file naming conventions.
    """

    @abc.abstractmethod
    def supports_sequencing(self) -> bool:
        """Denotes whether this path handler supports sequence-like indexes."""
        pass

    def get_full_path(self, parent: Path = Path()) -> Path:
        """Get the full path of the file."""
        return parent / self.get_folder_structure() / self.get_filename()

    @abc.abstractmethod
    def get_content_date_for_indexing(self) -> datetime | None:
        """Get the date of the file for indexing purposes."""
        pass

    @abc.abstractmethod
    def get_folder_structure(self) -> str:
        pass

    @abc.abstractmethod
    def get_filename(self) -> str:
        pass

    @abc.abstractmethod
    def add_metadata(self, metadata: dict) -> None:
        pass

    @abc.abstractmethod
    def get_metadata(self) -> dict | None:
        pass

    @classmethod
    @abc.abstractmethod
    def from_filename(cls: type[T], filename: str | Path) -> T | None:
        """Instantiate a path handler from a file name."""
        pass

    @classmethod
    def default_file_hash(cls, source_file):
        return hashlib.md5(source_file.read_bytes()).hexdigest()

    def get_content_identity(
        self, file_path_override: None | Path = None, parent_folder: Path = Path()
    ) -> str:
        """Return a hash representing content identity for deduplication.

        Override to use a different identity signal, e.g., a companion file's
        hash instead of the source file's own hash.
        """
        source_file = (
            file_path_override
            if file_path_override is not None
            else self.get_full_path(parent_folder)
        )
        return self.default_file_hash(source_file)

    def prepare_for_version(self, source_file: Path) -> Path:
        """Prepare the source file for the version currently set on this handler.

        Called after the final version is determined and before the file is copied
        into the datastore. Returns the path to copy from — may be a temporary
        rewrite of source_file. Callers must delete the returned file if it differs
        from source_file.

        Default: return source_file unchanged (no rewriting needed).
        """
        return source_file

    def is_version_blocked_by_sibling(
        self, version: int, datastore: Path, source_file: Path
    ) -> bool:
        """Return True if *version* must be skipped due to a sibling-file conflict.

        Called after the primary version-search loop to ensure paired files
        (e.g. a JSON layer and its companion CSV) always land on the same version.
        Override when a file type co-versions with another file type.

        Default: no sibling constraint.
        """
        return False

    def _check_property_values(
        self, method_description: str, properties: list[str]
    ) -> None:
        """
        Check if the required properties are set for a method.

        Raises ValueError if any of the properties are not set.
        """

        missing_properties = [
            f"'{prop}'" for prop in properties if not getattr(self, prop)
        ]

        if missing_properties:
            message = f"No {', '.join(missing_properties)} defined. Cannot generate {method_description}."

            logger.error(message)
            raise ValueError(message)
