import logging
from pathlib import Path

from imap_mag.io.AncillaryPathHandler import AncillaryPathHandler
from imap_mag.io.CalibrationLayerPathHandler import (
    CalibrationLayerPathHandler,
)
from imap_mag.io.HKPathHandler import HKPathHandler
from imap_mag.io.IFilePathHandler import IFilePathHandler
from imap_mag.io.SciencePathHandler import SciencePathHandler
from imap_mag.io.SPICEPathHandler import SPICEPathHandler

logger = logging.getLogger(__name__)


class NoProviderFoundError(Exception):
    """Exception raised when no suitable path handler is found."""

    def __init__(self, file: Path):
        super().__init__(f"No suitable path handler found for file {file}.")
        self.file = file


class FilePathHandlerSelector:
    """Manager of file path handlers."""

    @staticmethod
    def find_by_path(
        file: Path, *, throw_on_none_found: bool = True
    ) -> IFilePathHandler | None:
        """Find a suitable path handler for the given filepath."""

        # Providers to try in alphabetical order.
        provider_to_try: list[type[IFilePathHandler]] = [
            AncillaryPathHandler,
            CalibrationLayerPathHandler,
            HKPathHandler,
            SciencePathHandler,
            SPICEPathHandler,
        ]

        for provider in provider_to_try:
            path_handler = provider.from_filename(file)
            if path_handler:
                logger.debug(f"Path handler {provider.__name__} matches file {file}.")
                return path_handler

        if throw_on_none_found:
            logger.error(f"No suitable path handler found for file {file}.")
            raise NoProviderFoundError(file)
        else:
            logger.info(f"No suitable path handler found for file {file}.")
            return None
