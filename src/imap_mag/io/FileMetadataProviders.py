from pathlib import Path

from imap_mag.io.CalibrationLayerMetadataProvider import (
    CalibrationLayerMetadataProvider,
)
from imap_mag.io.IFileMetadataProvider import IFileMetadataProvider
from imap_mag.io.StandardSPDFMetadataProvider import StandardSPDFMetadataProvider


class NoProviderFoundError(Exception):
    """Exception raised when no suitable metadata provider is found."""

    def __init__(self, file: Path):
        super().__init__(f"No supported metadata provider found for file: {file}")
        self.file = file


class FileMetadataProviders:
    """Manager of file metadata providers."""

    @staticmethod
    def find_by_path(file: Path) -> IFileMetadataProvider:
        """Find a suitable metadata provider for the given filepath."""

        # Providers to try in order of precedence.
        provider_to_try: list[type[IFileMetadataProvider]] = [
            StandardSPDFMetadataProvider,
            CalibrationLayerMetadataProvider,
        ]

        for provider in provider_to_try:
            metadata_provider = provider.from_filename(file)
            if metadata_provider:
                return metadata_provider

        raise NoProviderFoundError(file)
