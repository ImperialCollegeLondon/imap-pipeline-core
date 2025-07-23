import glob
import logging
from pathlib import Path
from typing import Literal, overload

from imap_mag.io.file.IFilePathHandler import IFilePathHandler
from imap_mag.io.file.PartitionedPathHandler import PartitionedPathHandler
from imap_mag.io.file.SequenceablePathHandler import SequenceablePathHandler
from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler

logger = logging.getLogger(__name__)


class DatastoreFileFinder:
    """Find files in the datastore."""

    location: Path

    def __init__(self, location: Path) -> None:
        self.location = location

    def find_all_file_parts(
        self,
        path_handler: PartitionedPathHandler,
        throw_if_not_found: bool = False,
    ) -> list[Path]:
        """Get all files matching the path handler pattern."""

        all_matching_files: list[tuple[str, int]] = self.__find_files_and_sequences(
            path_handler, throw_if_not_found=throw_if_not_found
        )

        return [Path(file) for file, _ in all_matching_files]

    @overload
    def find_latest_version(
        self,
        path_handler: VersionedPathHandler,
        throw_if_not_found: Literal[True] = True,
    ) -> Path:
        pass

    @overload
    def find_latest_version(
        self,
        path_handler: VersionedPathHandler,
        throw_if_not_found: Literal[False] = False,
    ) -> Path | None:
        pass

    def find_latest_version(
        self,
        path_handler: VersionedPathHandler,
        throw_if_not_found: bool = True,
    ) -> Path | None:
        """Try to get file from data store, return None if not found."""

        all_matching_files: list[tuple[str, int]] = self.__find_files_and_sequences(
            path_handler, throw_if_not_found=throw_if_not_found
        )

        if not all_matching_files:
            return None

        (filename_with_sequence, _) = all_matching_files[0]
        return Path(filename_with_sequence)

    @overload
    def find_matching_file(
        self,
        path_handler: IFilePathHandler,
        throw_if_not_found: Literal[True] = True,
    ) -> Path:
        pass

    @overload
    def find_matching_file(
        self,
        path_handler: IFilePathHandler,
        throw_if_not_found: Literal[False] = False,
    ) -> Path | None:
        pass

    def find_matching_file(
        self,
        path_handler: IFilePathHandler,
        throw_if_not_found: bool = True,
    ) -> Path | None:
        matching_file = (
            self.location
            / path_handler.get_folder_structure()
            / path_handler.get_filename()
        )

        if matching_file.exists():
            return matching_file
        elif throw_if_not_found:
            logger.error(
                f"No file found matching {matching_file.name} in folder {matching_file.parent}."
            )
            raise FileNotFoundError(
                f"No file found matching {matching_file.name} in folder {matching_file.parent}."
            )
        else:
            return None

    def __find_files_and_sequences(
        self,
        path_handler: SequenceablePathHandler,
        throw_if_not_found: bool = True,
    ) -> list[tuple[str, int]]:
        pattern = path_handler.get_unsequenced_pattern()
        folder = self.location / path_handler.get_folder_structure()
        sequence_name = path_handler.get_sequence_variable_name()

        all_matching_files = [
            (filename, int(pattern.search(filename).group(sequence_name)))  # type: ignore
            for filename in glob.glob(folder.as_posix() + "/*")
            if pattern.search(filename)
        ]
        all_matching_files.sort(key=lambda x: x[1], reverse=True)

        if len(all_matching_files) == 0:
            if throw_if_not_found:
                logger.error(
                    f"No files found matching pattern {pattern.pattern} in folder {folder.as_posix()}."
                )
                raise FileNotFoundError(
                    f"No files found matching pattern {pattern.pattern} in folder {folder.as_posix()}."
                )
            else:
                logger.info(
                    f"No files found matching pattern {pattern.pattern} in folder {folder.as_posix()}."
                )
                return []

        return all_matching_files
