import abc
from pathlib import Path

from imap_mag.io.IFilePathHandler import IFilePathHandler, T


class IOutputManager(abc.ABC):
    """Interface for output managers."""

    @abc.abstractmethod
    def add_file(self, original_file: Path, path_handler: T) -> tuple[Path, T]:
        """Add file to output location."""

    @staticmethod
    def assemble_full_path(location: Path, path_handler: IFilePathHandler) -> Path:
        """Assemble full path from path handler."""

        return (
            location / path_handler.get_folder_structure() / path_handler.get_filename()
        )
