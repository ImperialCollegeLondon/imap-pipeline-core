import abc
import typing
from pathlib import Path

from imap_mag.io.IFileMetadataProvider import IFileMetadataProvider, T
from imap_mag.io.StandardSPDFMetadataProvider import StandardSPDFMetadataProvider


class IOutputManager(abc.ABC):
    """Interface for output managers."""

    @abc.abstractmethod
    def add_file(self, original_file: Path, metadata_provider: T) -> tuple[Path, T]:
        """Add file to output location."""

    def add_spdf_format_file(
        self, original_file: Path, **metadata: typing.Any
    ) -> tuple[Path, StandardSPDFMetadataProvider]:
        return self.add_file(original_file, StandardSPDFMetadataProvider(**metadata))

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
