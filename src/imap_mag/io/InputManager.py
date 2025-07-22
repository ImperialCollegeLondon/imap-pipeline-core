import glob
import logging
from pathlib import Path
from typing import Literal, overload

from imap_mag.io.IFilePathHandler import IFilePathHandler

logger = logging.getLogger(__name__)


class InputManager:
    """Manage input files."""

    location: Path

    def __init__(self, location: Path) -> None:
        self.location = location

    def find_all_file_sequences(
        self,
        path_handler: IFilePathHandler,
        throw_if_not_found: bool = False,
    ) -> list[Path]:
        """Get all files matching the path handler pattern."""

        all_matching_files: list[tuple[str, int]] = self.__find_files_and_sequences(
            path_handler, throw_if_not_found=throw_if_not_found
        )

        return [Path(file) for file, _ in all_matching_files]

    @overload
    def find_file_with_sequence(
        self,
        path_handler: IFilePathHandler,
        latest_sequence: bool = True,
        throw_if_not_found: Literal[True] = True,
    ) -> Path:
        pass

    @overload
    def find_file_with_sequence(
        self,
        path_handler: IFilePathHandler,
        latest_sequence: bool = True,
        throw_if_not_found: Literal[False] = False,
    ) -> Path | None:
        pass

    def find_file_with_sequence(
        self,
        path_handler: IFilePathHandler,
        latest_sequence: bool = True,
        throw_if_not_found: bool = True,
    ) -> Path | None:
        """Try to get file from data store, return None if not found."""

        all_matching_files: list[tuple[str, int]] = self.__find_files_and_sequences(
            path_handler, throw_if_not_found=throw_if_not_found
        )

        if not all_matching_files:
            return None

        if latest_sequence:
            (filename_with_sequence, _) = all_matching_files[0]
            return Path(filename_with_sequence)
        else:
            filename_with_sequence = next(
                filename
                for filename, v in all_matching_files
                if v == path_handler.sequence
            )
            return Path(filename_with_sequence)

    def __find_files_and_sequences(
        self,
        path_handler: IFilePathHandler,
        throw_if_not_found: bool = True,
    ) -> list[tuple[str, int]]:
        pattern = path_handler.get_unsequenced_pattern()
        folder = self.location / path_handler.get_folder_structure()

        all_matching_files = [
            (filename, int(pattern.search(filename).group("sequence")))  # type: ignore
            for filename in glob.glob(folder.as_posix() + "/*")
            if pattern.search(filename)
        ]
        all_matching_files.sort(key=lambda x: x[1], reverse=True)

        if len(all_matching_files) == 0:
            if throw_if_not_found:
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
