import logging
from pathlib import Path
from typing import Literal, overload

from imap_mag.io.file.AncillaryPathHandler import AncillaryPathHandler
from imap_mag.io.file.CalibrationLayerPathHandler import (
    CalibrationLayerPathHandler,
)
from imap_mag.io.file.HKBinaryPathHandler import HKBinaryPathHandler
from imap_mag.io.file.HKDecodedPathHandler import HKDecodedPathHandler
from imap_mag.io.file.IALiRTPathHandler import IALiRTPathHandler
from imap_mag.io.file.IFilePathHandler import IFilePathHandler
from imap_mag.io.file.SciencePathHandler import SciencePathHandler
from imap_mag.io.file.SPICEPathHandler import SPICEPathHandler

logger = logging.getLogger(__name__)


class NoProviderFoundError(Exception):
    """Exception raised when no suitable path handler is found."""

    def __init__(self, file: Path):
        super().__init__(f"No suitable path handler found for file {file}.")
        self.file = file


class FilePathHandlerSelector:
    """Manager of file path handlers."""

    @overload
    @staticmethod
    def find_by_path(
        file: Path, *, throw_if_not_found: Literal[True] = True
    ) -> IFilePathHandler:
        pass

    @overload
    @staticmethod
    def find_by_path(
        file: Path, *, throw_if_not_found: Literal[False] = False
    ) -> IFilePathHandler | None:
        pass

    @staticmethod
    def find_by_path(
        file: Path, *, throw_if_not_found: bool = True
    ) -> IFilePathHandler | None:
        """Find a suitable path handler for the given filepath."""

        # Providers to try in alphabetical order.
        provider_to_try: list[type[IFilePathHandler]] = [
            AncillaryPathHandler,
            CalibrationLayerPathHandler,
            HKBinaryPathHandler,
            HKDecodedPathHandler,
            IALiRTPathHandler,
            SciencePathHandler,
            SPICEPathHandler,
        ]

        for provider in provider_to_try:
            path_handler = provider.from_filename(file)
            if path_handler:
                logger.debug(f"Path handler {provider.__name__} matches file {file}.")
                return path_handler

        if throw_if_not_found:
            logger.error(f"No suitable path handler found for file {file}.")
            raise NoProviderFoundError(file)
        else:
            logger.info(f"No suitable path handler found for file {file}.")
            return None
