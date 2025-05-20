import logging
from pathlib import Path
from typing import Optional

import numpy as np

from imap_mag import appConfig
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


def getOutputManagerByMode(
    destination_folder: Path, use_database: bool
) -> IOutputManager:
    """use_databaseieve output manager based on destination and mode."""

    output_manager: IOutputManager = OutputManager(destination_folder)

    if use_database:
        return DatabaseFileOutputManager(output_manager)
    else:
        return output_manager


def copyFileToDestination(
    file_path: Path,
    destination: appConfig.Destination | Path,
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

    if isinstance(destination, appConfig.Destination):
        destination_fullpath = Path(destination.folder) / destination.filename
    else:
        destination_fullpath = destination

    if output_manager is None:
        output_manager = OutputManager(destination_fullpath.parent)

    return output_manager.add_file(
        file_path, SimpleMetadataProvider(destination_fullpath.name)
    )
