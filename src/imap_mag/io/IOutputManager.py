import abc
import typing
from pathlib import Path

from imap_mag.io.file.SequenceablePathHandler import SequenceablePathHandler

T = typing.TypeVar("T", bound="SequenceablePathHandler")


class IOutputManager(abc.ABC):
    """Interface for output managers."""

    @abc.abstractmethod
    def add_file(self, original_file: Path, path_handler: T) -> tuple[Path, T]:
        """Add file to output location."""

    @staticmethod
    def assemble_full_path(
        location: Path, path_handler: SequenceablePathHandler
    ) -> Path:
        """Assemble full path from path handler."""

        return (
            location / path_handler.get_folder_structure() / path_handler.get_filename()
        )
