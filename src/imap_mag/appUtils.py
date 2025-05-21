import logging
from pathlib import Path
from typing import Optional

from imap_mag import appConfig
from imap_mag.io import (
    DatabaseFileOutputManager,
    IFileMetadataProvider,
    IOutputManager,
    OutputManager,
)

logger = logging.getLogger(__name__)


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
