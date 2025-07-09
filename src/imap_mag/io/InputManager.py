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

    def get_all_file_versions(
        self,
        path_handler: IFilePathHandler,
        throw_if_none_found: bool = False,
    ) -> list[Path]:
        """Get all files matching the path handler pattern."""

        all_matching_files: list[tuple[str, int]] = self.__get_files_and_versions(
            path_handler, throw_if_none_found=throw_if_none_found
        )

        return [Path(file) for file, _ in all_matching_files]

    def get_versioned_file(
        self,
        path_handler: IFilePathHandler,
        latest_version: bool = True,
        throw_if_none_found: bool = True,
    ) -> Path | None:
        """Try to get file from data store, return None if not found."""

        all_matching_files: list[tuple[str, int]] = self.__get_files_and_versions(
            path_handler, throw_if_none_found=throw_if_none_found
        )

        if not all_matching_files:
            return None

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

    def __get_files_and_versions(
        self,
        path_handler: IFilePathHandler,
        throw_if_none_found: bool = True,
    ) -> list[tuple[str, int]]:
        pattern = path_handler.get_unversioned_pattern()
        folder = self.location / path_handler.get_folder_structure()

        all_matching_files = [
            (filename, int(pattern.search(filename).group("version")))  # type: ignore
            for filename in glob.glob(folder.as_posix() + "/*")
            if pattern.search(filename)
        ]
        all_matching_files.sort(key=lambda x: x[1], reverse=True)

        if len(all_matching_files) == 0:
            if throw_if_none_found:
                logger.error(
                    f"No files found matching pattern {pattern.pattern} in folder {folder.as_posix()}"
                )
                raise FileNotFoundError(
                    f"No files found matching pattern {pattern.pattern} in folder {folder.as_posix()}"
                )
            else:
                logger.info(
                    f"No files found matching pattern {pattern.pattern} in folder {folder.as_posix()}"
                )
                return []

        return all_matching_files
