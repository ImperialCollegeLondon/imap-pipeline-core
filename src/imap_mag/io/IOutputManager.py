import abc
from pathlib import Path

from imap_mag.io.IFileMetadataProvider import IFileMetadataProvider, T


class IOutputManager(abc.ABC):
    """Interface for output managers."""

    @abc.abstractmethod
    def add_file(self, original_file: Path, metadata_provider: T) -> tuple[Path, T]:
        """Add file to output location."""

    @staticmethod
    def assemble_full_path(
        location: Path, metadata_provider: IFileMetadataProvider
    ) -> Path:
        """Assemble full path from metadata."""

        return (
            location
            / metadata_provider.get_folder_structure()
            / metadata_provider.get_filename()
        )
