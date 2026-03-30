import fnmatch
import glob
import logging
from datetime import datetime
from pathlib import Path
from typing import Literal, overload

from imap_mag.io.file import (
    CalibrationLayerPathHandler,
    IFilePathHandler,
    PartitionedPathHandler,
    SciencePathHandler,
    SequenceablePathHandler,
    VersionedPathHandler,
)
from imap_mag.util import ScienceMode

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

    def resolve_layer_patterns(
        self,
        layers: list[str],
        start_date: datetime,
    ) -> list[str]:
        """Resolve layer pattern strings to actual layer filenames.

        Each entry in layers can be:
        - An exact filename (e.g. "imap_mag_noop-layer_20260116_v001.json")
        - A glob pattern (e.g. "*noop*", "*") that matches layer filenames for the given date.

        When matching by pattern, only the highest version per descriptor+date is returned.

        Returns resolved filenames in the order the patterns were provided.
        """
        handler = CalibrationLayerPathHandler(content_date=start_date, descriptor="x")
        layer_dir = self.location / handler.get_folder_structure()
        date_str = start_date.strftime("%Y%m%d")

        resolved: list[str] = []
        for layer in layers:
            if "*" in layer or "?" in layer:
                if not layer_dir.exists():
                    logger.warning(
                        f"Layer directory {layer_dir} does not exist, skipping pattern {layer}"
                    )
                    continue
                matched = []
                for f in sorted(layer_dir.iterdir()):
                    if (
                        f.is_file()
                        and f.suffix == ".json"
                        and date_str in f.name
                        and fnmatch.fnmatch(f.name, layer)
                    ):
                        matched.append(f.name)

                matched = self._keep_highest_versions(matched)

                if not matched:
                    logger.warning(
                        f"No layer files matched pattern '{layer}' for date {date_str} in {layer_dir}"
                    )
                resolved.extend(matched)
            else:
                resolved.append(layer)

        return resolved

    @staticmethod
    def _keep_highest_versions(filenames: list[str]) -> list[str]:
        """Given a list of layer filenames, keep only the highest version per descriptor+date."""
        best: dict[str, tuple[str, int]] = {}
        for name in filenames:
            handler = CalibrationLayerPathHandler.from_filename(name)
            if handler is None:
                continue
            key = f"{handler.descriptor}_{handler.content_date}"
            if key not in best or handler.version > best[key][1]:
                best[key] = (name, handler.version)
        return [name for name, _ in best.values()]

    def find_science_file(self, start_date: datetime, mode: ScienceMode) -> str:
        """Find the highest version science file for a given date and mode.

        For burst mode, only L1B files are used.
        For normal mode, L1C files are preferred; falls back to L1B if no L1C exists.

        Returns the filename of the highest version match.
        """
        date_str = start_date.strftime("%Y%m%d")
        science_dir = self.location / "science" / "mag"

        if not science_dir.exists():
            raise FileNotFoundError(f"Science directory {science_dir} does not exist")

        if mode == ScienceMode.Burst:
            levels_to_search = ["l1b"]
        else:
            levels_to_search = ["l1c", "l1b"]

        for level in levels_to_search:
            date_dir = (
                science_dir
                / level
                / start_date.strftime("%Y")
                / start_date.strftime("%m")
            )
            if not date_dir.exists():
                continue

            candidates: list[tuple[str, int]] = []
            for f in sorted(date_dir.iterdir()):
                if not f.is_file() or f.suffix != ".cdf":
                    continue
                if date_str not in f.name:
                    continue

                handler = SciencePathHandler.from_filename(f.name)
                if handler and handler.get_mode() == mode:
                    candidates.append((f.name, handler.version))

            if candidates:
                candidates.sort(key=lambda x: x[1], reverse=True)
                logger.info(
                    f"Discovered science file {candidates[0][0]} for date {start_date.strftime('%Y-%m-%d')} mode {mode.value} at level {level}"
                )
                return candidates[0][0]

        raise FileNotFoundError(
            f"No science file found for date {start_date.strftime('%Y-%m-%d')} and mode {mode.value}"
        )

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
                logger.debug(
                    f"No files found matching pattern {pattern.pattern} in folder {folder.as_posix()}."
                )
                return []

        return all_matching_files
