import logging
from pathlib import Path

from imap_mag.io import (
    DatabaseFileOutputManager,
    IOutputManager,
    OutputManager,
)
from imap_mag.io.file import IFilePathHandler

logger = logging.getLogger(__name__)


def getOutputManagerByMode(
    destination_folder: Path, use_database: bool
) -> IOutputManager:
    """Retrieve output manager based on destination and mode."""

    output_manager: IOutputManager = OutputManager(destination_folder)

    if use_database:
        return DatabaseFileOutputManager(output_manager)
    else:
        return output_manager


def copyFileToDestination(
    file_path: Path,
    destination: Path,
    output_manager: OutputManager | None = None,
) -> tuple[Path, IFilePathHandler]:
    """Copy file to destination folder."""

    class SimplePathHandler(IFilePathHandler):
        """Simple path handler for compatibility."""

        def __init__(self, filename: str) -> None:
            self.filename = filename

        def supports_sequencing(self) -> bool:
            return False

        def get_folder_structure(self) -> str:
            return ""

        def get_filename(self) -> str:
            return self.filename

        @classmethod
        def from_filename(cls, filename: Path | str) -> "SimplePathHandler | None":
            return cls(str(filename))

    if output_manager is None:
        output_manager = OutputManager(destination.parent)

    return output_manager.add_file(file_path, SimplePathHandler(destination.name))
