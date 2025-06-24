from pathlib import Path

from imap_mag.io.CalibrationLayerMetadataProvider import (
    CalibrationLayerMetadataProvider,
)
from imap_mag.io.IFileMetadataProvider import IFileMetadataProvider
from imap_mag.io.StandardSPDFMetadataProvider import StandardSPDFMetadataProvider


def find_supported_provider(file: Path) -> IFileMetadataProvider:
    """Find a suitable metadata provider for the given file."""

    # Providers to try in order of precedence.
    provider_to_try: list[type[IFileMetadataProvider]] = [
        StandardSPDFMetadataProvider,
        CalibrationLayerMetadataProvider,
    ]

    for provider in provider_to_try:
        metadata_provider = provider.from_filename(file)
        if metadata_provider:
            return metadata_provider

    raise ValueError(f"No supported metadata provider found for file: {file}")
