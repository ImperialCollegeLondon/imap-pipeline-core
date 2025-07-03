import logging
from pathlib import Path

from imap_mag.io.AncillaryFileMetadataProvider import AncillaryFileMetadataProvider
from imap_mag.io.CalibrationLayerMetadataProvider import (
    CalibrationLayerMetadataProvider,
)
from imap_mag.io.HKMetadataProvider import HKMetadataProvider
from imap_mag.io.IFileMetadataProvider import IFileMetadataProvider
from imap_mag.io.ScienceMetadataProvider import ScienceMetadataProvider

logger = logging.getLogger(__name__)


class NoProviderFoundError(Exception):
    """Exception raised when no suitable metadata provider is found."""

    def __init__(self, file: Path):
        super().__init__(f"No suitable metadata provider found for file {file}.")
        self.file = file


class FileMetadataProviderSelector:
    """Manager of file metadata providers."""

    @staticmethod
    def find_by_path(
        file: Path, *, throw_on_none_found: bool = True
    ) -> IFileMetadataProvider | None:
        """Find a suitable metadata provider for the given filepath."""

        # Providers to try in alphabetical order.
        provider_to_try: list[type[IFileMetadataProvider]] = [
            AncillaryFileMetadataProvider,
            CalibrationLayerMetadataProvider,
            HKMetadataProvider,
            ScienceMetadataProvider,
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
