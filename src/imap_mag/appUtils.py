import logging
from pathlib import Path
from typing import Optional

import numpy as np

from imap_mag import appConfig
from imap_mag.config.FetchMode import FetchMode
from imap_mag.io import (
    DatabaseFileOutputManager,
    IFileMetadataProvider,
    IOutputManager,
    OutputManager,
)
from imap_mag.util import CONSTANTS

logger = logging.getLogger(__name__)


def convertMETToJ2000ns(
    met: np.typing.ArrayLike,
    reference_epoch: np.datetime64 = CONSTANTS.IMAP_EPOCH,
) -> np.typing.ArrayLike:
    """Convert mission elapsed time (MET) to nanoseconds from J2000."""
    time_array = (np.asarray(met, dtype=float) * 1e9).astype(np.int64)
    j2000_offset = (
        (reference_epoch - CONSTANTS.J2000_EPOCH)
        .astype("timedelta64[ns]")
        .astype(np.int64)
    )
    return j2000_offset + time_array


# TODO: Replace all uses of this with getOutputManagerByMode version
def getOutputManager(destination: appConfig.Destination) -> IOutputManager:
    """Retrieve output manager based on destination."""

    output_manager: IOutputManager = OutputManager(destination.folder)

    if destination.export_to_database:
        output_manager = DatabaseFileOutputManager(output_manager)

    return output_manager


def getOutputManagerByMode(destination_folder: Path, mode: FetchMode) -> IOutputManager:
    """Retrieve output manager based on destination and mode."""

    if mode == FetchMode.DownloadOnly:
        return OutputManager(destination_folder)
    elif mode == FetchMode.DownloadAndUpdateProgress:
        return DatabaseFileOutputManager(OutputManager(destination_folder))
    else:
        raise ValueError(f"Unsupported mode: {mode}")


def copyFileToDestination(
    file_path: Path,
    destination: appConfig.Destination,
    output_manager: Optional[OutputManager] = None,
) -> tuple[Path, IFileMetadataProvider]:
    """Copy file to destination folder."""

    class SimpleMetadataProvider(IFileMetadataProvider):
        """Simple metadata provider for compatibility."""

        def __init__(self, filename: str) -> None:
            self.filename = filename

        def supports_versioning(self) -> bool:
            return False

        def get_folder_structure(self) -> str:
            return ""

        def get_filename(self) -> str:
            return self.filename

    destination_folder = Path(destination.folder)

    if output_manager is None:
        output_manager = OutputManager(destination_folder)

    return output_manager.add_file(
        file_path, SimpleMetadataProvider(destination.filename)
    )
