import glob
import logging
from pathlib import Path

from imap_mag.io import StandardSPDFMetadataProvider

logger = logging.getLogger(__name__)


class InputManager:
    """Manage input files."""

    location: Path

    def __init__(self, location: Path) -> None:
        self.location = location

    def get_versioned_file(
        self,
        metadata_provider: StandardSPDFMetadataProvider,
        latest_version: bool = True,
    ) -> Path:
        """Get file from data store."""

        if not metadata_provider.content_date:
            logger.error("No 'content_date' defined. Cannot generate filename")
            raise ValueError("No 'content_date' defined. Cannot generate filename.")

        pattern = metadata_provider.get_unversioned_pattern()

        folder = self.location / metadata_provider.get_folder_structure()

        all_matching_files = [
            (filename, int(pattern.search(filename).group("version")))
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
                if v == metadata_provider.version
            )
            return Path(versioned_filename)
