import fnmatch
import glob
import logging
from datetime import datetime
from pathlib import Path
from typing import Literal, overload

from imap_mag.db.Database import Database
from imap_mag.io.file import (
    CalibrationLayerPathHandler,
    IFilePathHandler,
    PartitionedPathHandler,
    SciencePathHandler,
    SequenceablePathHandler,
    VersionedPathHandler,
)
from imap_mag.io.FilePathHandlerSelector import FilePathHandlerSelector
from imap_mag.util import MAGSensor, ScienceMode

logger = logging.getLogger(__name__)


class FileFinder:
    """Find files in the datastore."""

    _data_store: Path
    _work_folder: Path | None
    _database: Database | None

    def __init__(
        self,
        data_store: Path,
        work_folder: Path | None = None,
        database: Database | None = None,
    ) -> None:
        self._data_store = data_store
        self._work_folder = work_folder
        self._database = database

    def find_parts_by_handler(
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
    def find_latest_version_by_handler(
        self,
        path_handler: VersionedPathHandler,
        throw_if_not_found: Literal[True] = True,
    ) -> Path:
        pass

    @overload
    def find_latest_version_by_handler(
        self,
        path_handler: VersionedPathHandler,
        throw_if_not_found: Literal[False] = False,
    ) -> Path | None:
        pass

    def find_latest_version_by_handler(
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
    def find_by_handler(
        self,
        path_handler: IFilePathHandler,
        throw_if_not_found: Literal[True] = True,
    ) -> Path:
        pass

    @overload
    def find_by_handler(
        self,
        path_handler: IFilePathHandler,
        throw_if_not_found: Literal[False] = False,
    ) -> Path | None:
        pass

    def find_by_handler(
        self,
        path_handler: IFilePathHandler,
        throw_if_not_found: bool = True,
    ) -> Path | None:
        matching_file = (
            self._data_store
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

    def find_layers_by_date_and_patterns(
        self,
        layers: list[str],
        date: datetime,
        mode: ScienceMode,
        throw_if_not_found: bool = False,
    ) -> list[str]:
        """Resolve layer pattern strings to actual layer filenames. Expect one layer file per item in layers.

        Each entry in layers can be:
        - An exact filename (e.g. "imap_mag_noop-norm-layer_20260116_v001.json")
        - A glob pattern (e.g. "*noop*", "*") that matches layer filenames for the given date.

        When matching by pattern, only the highest version per descriptor+date is returned.

        Returns resolved filenames in the order the patterns were provided.
        """
        handler = CalibrationLayerPathHandler(
            content_date=date,
            descriptor=CalibrationLayerPathHandler.DESCRIPTOR_WILDCARD,
        )

        resolved: list[str] = []
        for layer in layers:
            fnmatch_pattern = None
            if "*" in layer or "?" in layer:
                fnmatch_pattern = layer

            all_matching_files_with_version: list[tuple[str, int]] = (
                self.__find_files_and_sequences(
                    handler,
                    throw_if_not_found=throw_if_not_found,
                    fnmatch_pattern=fnmatch_pattern,
                    filename_only=True,
                )
            )

            if not fnmatch_pattern:
                # must match exactly by filename, so filter to that file only
                all_matching_files_with_version = [
                    (f, v)
                    for f, v in all_matching_files_with_version
                    if Path(f).name == layer
                ]

            # remove files that do not match mode "{mode}-layer"
            all_matching_files_with_version = [
                (f, v)
                for f, v in all_matching_files_with_version
                if f"{mode.short_name}-layer" in f
            ]

            if all_matching_files_with_version:
                if layer == "*":
                    resolved.extend(
                        self._keep_highest_version_layers_only(
                            [f for f, v in all_matching_files_with_version]
                        )
                    )
                else:  # just take the highest version match
                    resolved.append(all_matching_files_with_version[0][0])
            elif throw_if_not_found:
                logger.error(
                    f"No layer files found matching pattern '{layer}' for date {date.strftime('%Y-%m-%d')} in {handler.get_folder_structure()}."
                )
                raise FileNotFoundError(
                    f"No layer files found matching pattern '{layer}' for date {date.strftime('%Y-%m-%d')} in {handler.get_folder_structure()}."
                )

        return resolved

    @staticmethod
    def _keep_highest_version_layers_only(filenames: list[str]) -> list[str]:
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

    def find_latests_science_by_date(
        self,
        date: datetime,
        mode: ScienceMode,
        sensor: MAGSensor,
        levels_to_search=["l1c", "l1b"],
    ) -> str:
        """Find the highest version science file for a given date and mode.
        Returns the filename of the highest version match.
        """
        date_str = date.strftime("%Y%m%d")
        science_dir = self._data_store / "science" / "mag"

        if not science_dir.exists():
            raise FileNotFoundError(f"Science directory {science_dir} does not exist")

        if mode == ScienceMode.Burst:
            # Remove l1c from levels to search for burst mode as no BM in L1C
            levels_to_search = [level for level in levels_to_search if level != "l1c"]

        for level in levels_to_search:
            date_dir = science_dir / level / date.strftime("%Y") / date.strftime("%m")
            if not date_dir.exists():
                continue

            candidates: list[tuple[str, int]] = []
            for f in sorted(date_dir.iterdir()):
                if not f.is_file() or f.suffix != ".cdf" or date_str not in f.name:
                    continue

                handler = SciencePathHandler.from_filename(f.name)
                if (
                    handler
                    and handler.get_mode() == mode
                    and handler.get_sensor() == sensor
                ):
                    candidates.append((f.name, handler.version))

            if candidates:
                candidates.sort(key=lambda x: x[1], reverse=True)
                logger.info(
                    f"Discovered science file {candidates[0][0]} for {sensor.value} date {date.strftime('%Y-%m-%d')} mode {mode.value} at level {level}"
                )
                return candidates[0][0]

        raise FileNotFoundError(
            f"No science file found for date {date.strftime('%Y-%m-%d')} and mode {mode.value}"
        )

    def __find_files_and_sequences(
        self,
        path_handler: SequenceablePathHandler,
        fnmatch_pattern: str | None = None,
        throw_if_not_found: bool = True,
        filename_only: bool = False,
    ) -> list[tuple[str, int]]:
        pattern = path_handler.get_unsequenced_pattern()
        folder = self._data_store / path_handler.get_folder_structure()
        sequence_name = path_handler.get_sequence_variable_name()

        all_matching_files = [
            (filename, int(pattern.search(filename).group(sequence_name)))  # type: ignore
            for filename in glob.glob(folder.as_posix() + "/*")
            if pattern.search(filename)
        ]

        if fnmatch_pattern is not None:
            all_matching_files = [
                (filename, seq)
                for filename, seq in all_matching_files
                if fnmatch.fnmatch(Path(filename).name, fnmatch_pattern)
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

        if filename_only:
            return [
                (Path(filename).name, version)
                for filename, version in all_matching_files
            ]

        return all_matching_files

    @overload
    def find_by_name_or_path(
        self, file_name_or_path: "str | Path", throw_if_not_found: Literal[True] = True
    ) -> "Path":
        pass

    @overload
    def find_by_name_or_path(
        self,
        file_name_or_path: "str | Path",
        throw_if_not_found: Literal[False] = False,
    ) -> "Path | None":
        pass

    def find_by_name_or_path(
        self, file_name_or_path: "str | Path", throw_if_not_found: bool = False
    ) -> "Path | None":
        path = (
            Path(file_name_or_path)
            if isinstance(file_name_or_path, str)
            else file_name_or_path
        )

        # path already exists → return immediately
        if path.exists():
            logger.debug("Resolved '%s' via existing path.", file_name_or_path)
            return path

        # Relative path from data_store
        candidate = self._data_store / path
        if candidate.exists():
            logger.debug(
                "Resolved '%s' relative to data_store: %s", file_name_or_path, candidate
            )
            return candidate

        # Relative path from _work_folder
        if self._work_folder is not None:
            candidate = self._work_folder / path
            if candidate.exists():
                logger.debug(
                    "Resolved '%s' relative to work_folder: %s",
                    file_name_or_path,
                    candidate,
                )
                return candidate

        # Steps that apply only to bare filenames (no directory component).
        if path.parent == Path("."):
            filename = path.name

            # try using the path handlers
            handler = FilePathHandlerSelector.find_by_path(
                path, throw_if_not_found=False
            )
            if handler is not None:
                candidate = (
                    self._data_store
                    / handler.get_folder_structure()
                    / handler.get_filename()
                )
                if candidate.exists():
                    logger.debug(
                        "Resolved '%s' via path handler in data_store: %s",
                        file_name_or_path,
                        candidate,
                    )
                    return candidate

            # try using the database (if available)
            if self._database is not None:
                try:
                    files = self._database.get_files(name=filename, deletion_date=None)
                    if files:
                        db_path = self._data_store / files[0].path
                        if db_path.exists():
                            logger.debug(
                                "Resolved '%s' via database record: %s",
                                file_name_or_path,
                                db_path,
                            )
                            return db_path
                except Exception:
                    logger.error(
                        "Database lookup failed for '%s'.",
                        file_name_or_path,
                        exc_info=True,
                    )
                    raise

        if throw_if_not_found:
            raise FileNotFoundError(
                f"File not found: '{file_name_or_path}'. "
                f"Searched: local path, "
                f"data_store ({self._data_store}), "
                f"CWD ({Path.cwd()}), "
                f"work_folder ({self._work_folder}), "
                f"file path handlers, and database."
            )
        else:
            return None
