import logging
from pathlib import Path

from imap_mag.io import AncillaryFileMetadataProvider
from imap_mag.io.CalibrationLayerMetadataProvider import (
    CalibrationLayerMetadataProvider,
)
from imap_mag.io.IFileMetadataProvider import IFileMetadataProvider
from imap_mag.io.StandardSPDFMetadataProvider import StandardSPDFMetadataProvider

logger = logging.getLogger(__name__)


class NoProviderFoundError(Exception):
    """Exception raised when no suitable metadata provider is found."""

    def __init__(self, file: Path):
        super().__init__(f"No suitable metadata provider found for file {file}.")
        self.file = file


class FileMetadataProviders:
    """Manager of file metadata providers."""

    @staticmethod
    def find_by_path(
        file: Path, *, throw_on_none_found: bool = True
    ) -> IFileMetadataProvider | None:
        """Find a suitable metadata provider for the given filepath."""

        # Providers to try in order of precedence.
        provider_to_try: list[type[IFileMetadataProvider]] = [
            AncillaryFileMetadataProvider,
            StandardSPDFMetadataProvider,
            CalibrationLayerMetadataProvider,
        ]

        for provider in provider_to_try:
            metadata_provider = provider.from_filename(file)
            if metadata_provider:
                logger.debug(
                    f"Metadata provider {provider.__name__} matches file {file}."
                )
                return metadata_provider

        if throw_on_none_found:
            logger.error(f"No suitable metadata provider found for file {file}.")
            raise NoProviderFoundError(file)
        else:
            logger.info(f"No suitable metadata provider found for file {file}.")
            return None
