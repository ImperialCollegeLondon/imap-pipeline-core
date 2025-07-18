import glob
import logging
from pathlib import Path

from imap_mag.io.IFilePathHandler import IFilePathHandler

logger = logging.getLogger(__name__)


class InputManager:
    """Manage input files."""

    location: Path

    def __init__(self, location: Path) -> None:
        self.location = location

    def get_versioned_file(
        self,
        path_handler: IFilePathHandler,
        latest_version: bool = True,
    ) -> Path:
        """Get file from data store."""

        pattern = path_handler.get_unversioned_pattern()

        folder = self.location / path_handler.get_folder_structure()

        all_matching_files = [
            (filename, int(pattern.search(filename).group("version")))  # type: ignore
            for filename in glob.glob(folder.as_posix() + "/*")
            if pattern.search(filename)
        ]
        all_matching_files.sort(key=lambda x: x[1], reverse=True)

        if len(all_matching_files) == 0:
            logger.error(
                f"No files found matching pattern {pattern.pattern} in folder {folder.as_posix()}"
            )
            raise FileNotFoundError(
                f"No files found matching pattern {pattern.pattern} in folder {folder.as_posix()}"
            )

        if latest_version:
            (versioned_filename, _) = all_matching_files[0]
            return Path(versioned_filename)
        else:
            versioned_filename = next(
                filename
                for filename, v in all_matching_files
                if v == path_handler.version
            )
            return Path(versioned_filename)
