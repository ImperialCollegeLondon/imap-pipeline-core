import fnmatch
import glob
import logging
import re
from datetime import datetime, timedelta
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
from imap_mag.util.DatetimeProvider import DatetimeProvider

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

    def find_latest_science_by_date(
        self,
        date: datetime,
        mode: ScienceMode,
        sensor: MAGSensor,
        levels_to_search=["l1c", "l1b"],
        version_major: int | None = None,
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

            if version_major is not None:
                candidates = [
                    (name, ver)
                    for name, ver in candidates
                    if (h := SciencePathHandler.from_filename(name)) is not None
                    and h.version_major == version_major
                ]

            if candidates:
                candidates.sort(key=lambda x: x[1], reverse=True)
                logger.info(
                    f"Discovered science file {candidates[0][0]} for {sensor.value} date {date.strftime('%Y-%m-%d')} mode {mode.value} at level {level}"
                )
                return candidates[0][0]

        raise FileNotFoundError(
            f"No science file found for date {date.strftime('%Y-%m-%d')} and mode {mode.value}"
        )

    _COVERAGE_PLACEHOLDERS = frozenset({"from_doy", "to_doy", "sequence"})
    _COVERAGE_PLACEHOLDER_RE = re.compile(r"\{from_doy\}|\{to_doy\}|\{sequence\}")
    _PLACEHOLDER_RE = re.compile(r"\{(\w+)\}")

    def find_matching_files(
        self,
        relative_pattern: str,
        start_date: datetime,
        end_date: datetime,
        days_before: int = 0,
        days_after: int = 0,
        highest_sequence_only: bool = False,
        get_previous_if_empty: bool = False,
    ) -> list[Path]:
        """Find files matching ``relative_pattern`` (relative to ``data_store``)
        that cover the search window ``[start_date - days_before, end_date + days_after]``.

        ``relative_pattern`` is either:
        - A day-of-year coverage-window pattern, using ``{from_doy}``/``{to_doy}``
          placeholders for the ``{year}_{doy}`` coverage-window start/end encoded
          in the filename (e.g. ``imap_{from_doy}_{to_doy}_hist_{sequence}.sff``
          matches ``imap_2026_165_2026_166_hist_01.sff``), and an optional
          ``{sequence}`` placeholder for a per-window sequence number. Files are
          returned if their coverage window overlaps the search window at all.
        - A dated pattern, using ``strftime`` codes to fill in the date per day
          in the search window (e.g. ``imap_..._%Y%m%d_v*.csv``).

        ``highest_sequence_only`` keeps only the highest-sequence file per
        distinct coverage window (coverage-window patterns), or, for dated
        patterns, the single highest-sequence file per day, where the sequence
        (e.g. version) to compare is identified by a required ``{sequence}``
        placeholder in the pattern (e.g. ``..._v{sequence}.csv``) - a
        ``ValueError`` is raised if a dated pattern is used with
        ``highest_sequence_only=True`` but has no ``{sequence}`` placeholder.

        ``get_previous_if_empty`` falls back to the most recent match before the
        search window when nothing is found within it - useful for point-in-time
        state files that are only regenerated occasionally, where the state as of
        the start of the window is still needed. For coverage-window patterns this
        is the file(s) with the latest coverage-window end before the search
        window; for dated patterns it is the most recent day with a match.
        """
        self._validate_pattern_placeholders(relative_pattern)

        if "{from_doy}" in relative_pattern:
            return self._find_by_coverage_window(
                relative_pattern,
                start_date,
                end_date,
                days_before,
                days_after,
                highest_sequence_only,
                get_previous_if_empty,
            )

        return self._find_dated_files(
            relative_pattern,
            start_date,
            end_date,
            days_before,
            days_after,
            highest_sequence_only,
            get_previous_if_empty,
        )

    @classmethod
    def _validate_pattern_placeholders(cls, pattern: str) -> None:
        unsupported = (
            set(cls._PLACEHOLDER_RE.findall(pattern)) - cls._COVERAGE_PLACEHOLDERS
        )
        if unsupported:
            raise ValueError(
                f"Pattern '{pattern}' contains unsupported placeholder(s): "
                f"{', '.join(sorted(unsupported))}. Supported placeholders are: "
                f"{', '.join(sorted(cls._COVERAGE_PLACEHOLDERS))}."
            )

    def _find_by_coverage_window(
        self,
        relative_pattern: str,
        start_date: datetime,
        end_date: datetime,
        days_before: int,
        days_after: int,
        highest_sequence_only: bool,
        get_previous_if_empty: bool,
    ) -> list[Path]:
        search_start = start_date - timedelta(days=days_before)
        search_end = end_date + timedelta(days=days_after)

        candidates = self._coverage_window_candidates(relative_pattern)

        matching = [
            candidate
            for candidate in candidates
            if candidate[1] <= search_end and candidate[2] >= search_start
        ]

        if not matching and get_previous_if_empty:
            previous = [
                candidate for candidate in candidates if candidate[2] < search_start
            ]
            if previous:
                latest_end = max(candidate[2] for candidate in previous)
                matching = [
                    candidate for candidate in previous if candidate[2] == latest_end
                ]

        return self._select_by_sequence(matching, highest_sequence_only)

    def _coverage_window_candidates(
        self, relative_pattern: str
    ) -> list[tuple[Path, datetime, datetime, int]]:
        pattern = self._coverage_window_regex(Path(relative_pattern).name)
        glob_pattern = self._COVERAGE_PLACEHOLDER_RE.sub("*", relative_pattern)

        candidates: list[tuple[Path, datetime, datetime, int]] = []
        for path in sorted(self._data_store.glob(glob_pattern)):
            if not path.is_file():
                continue

            match = pattern.match(path.name)
            if not match:
                continue

            groups = match.groupdict()
            window_start = datetime.strptime(
                f"{groups['from_year']}{groups['from_doy']}", "%Y%j"
            )
            window_end = datetime.strptime(
                f"{groups['to_year']}{groups['to_doy']}", "%Y%j"
            )
            sequence = int(groups["sequence"]) if "sequence" in groups else 0
            candidates.append((path, window_start, window_end, sequence))
        return candidates

    @staticmethod
    def _select_by_sequence(
        candidates: list[tuple[Path, datetime, datetime, int]],
        highest_sequence_only: bool,
    ) -> list[Path]:
        if not highest_sequence_only:
            return [path for path, _, _, _ in candidates]

        best: dict[tuple[datetime, datetime], tuple[Path, int]] = {}
        for path, window_start, window_end, sequence in candidates:
            key = (window_start, window_end)
            if key not in best or sequence > best[key][1]:
                best[key] = (path, sequence)
        return sorted(path for path, _ in best.values())

    @staticmethod
    def _coverage_window_regex(pattern: str) -> re.Pattern:
        """Build a regex that captures the DOY window (and optional sequence)
        encoded in filenames matching ``pattern``."""
        parts = re.split(r"(\{from_doy\}|\{to_doy\}|\{sequence\})", pattern)
        regex_parts = []
        for part in parts:
            if part == "{from_doy}":
                regex_parts.append(r"(?P<from_year>\d{4})_(?P<from_doy>\d{3})")
            elif part == "{to_doy}":
                regex_parts.append(r"(?P<to_year>\d{4})_(?P<to_doy>\d{3})")
            elif part == "{sequence}":
                regex_parts.append(r"(?P<sequence>\d+)")
            else:
                regex_parts.append(re.escape(part))
        return re.compile("^" + "".join(regex_parts) + "$")

    def _find_dated_files(
        self,
        relative_pattern: str,
        start_date: datetime,
        end_date: datetime,
        days_before: int,
        days_after: int,
        highest_sequence_only: bool,
        get_previous_if_empty: bool,
    ) -> list[Path]:
        if highest_sequence_only:
            self._require_sequence_placeholder(relative_pattern)

        search_start = (start_date - timedelta(days=days_before)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        search_end = (end_date + timedelta(days=days_after)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        matches: list[Path] = []
        day = search_start
        while day <= search_end:
            matches.extend(
                self._glob_dated_files(day, relative_pattern, highest_sequence_only)
            )
            day += timedelta(days=1)

        if matches or not get_previous_if_empty:
            return matches

        earliest = DatetimeProvider().beginning_of_imap()
        day = search_start - timedelta(days=1)
        while day >= earliest:
            found = self._glob_dated_files(day, relative_pattern, highest_sequence_only)
            if found:
                return found
            day -= timedelta(days=1)

        return []

    @staticmethod
    def _require_sequence_placeholder(relative_pattern: str) -> None:
        if "{sequence}" not in relative_pattern:
            raise ValueError(
                f"Pattern '{relative_pattern}' has highest_sequence_only=True but no "
                "'{sequence}' placeholder to identify the version to compare - add "
                "one (e.g. '..._v{sequence}.csv') or set highest_sequence_only=False."
            )

    def _glob_dated_files(
        self, day: datetime, relative_pattern: str, highest_sequence_only: bool
    ) -> list[Path]:
        """Glob the files matching ``relative_pattern`` for a single ``day``.

        If ``highest_sequence_only``, ``{sequence}`` in the pattern identifies the
        version/sequence number to compare, and only the highest-sequence file is
        returned (other wildcards, e.g. sensor, are treated as part of the day's
        one result rather than distinguishing separate entities)."""
        dated_pattern = day.strftime(relative_pattern)

        if not highest_sequence_only:
            return self._glob_files(dated_pattern.replace("{sequence}", "*"))

        regex = self._dated_sequence_regex(Path(dated_pattern).name)
        glob_pattern = dated_pattern.replace("{sequence}", "*")

        candidates: list[tuple[Path, int]] = [
            (path, int(match.group("sequence")))
            for path in self._glob_files(glob_pattern)
            if (match := regex.match(path.name))
        ]
        if not candidates:
            return []

        highest = max(sequence for _, sequence in candidates)
        return sorted(path for path, sequence in candidates if sequence == highest)

    @staticmethod
    def _dated_sequence_regex(filename_pattern: str) -> re.Pattern:
        """Build a regex that captures the ``{sequence}`` value encoded in
        filenames matching ``filename_pattern`` (a single day's dated pattern,
        basename only), treating any other ``*`` as an unconstrained wildcard."""
        parts = re.split(r"(\{sequence\}|\*)", filename_pattern)
        regex_parts = [
            r"(?P<sequence>\d+)"
            if part == "{sequence}"
            else ".*"
            if part == "*"
            else re.escape(part)
            for part in parts
        ]
        return re.compile("^" + "".join(regex_parts) + "$")

    def _glob_files(self, glob_pattern: str) -> list[Path]:
        return sorted(
            path for path in self._data_store.glob(glob_pattern) if path.is_file()
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

        # Relative path from data_store
        candidate = self._data_store / path
        if candidate.exists():
            logger.debug(
                "Resolved '%s' relative to data_store: %s", file_name_or_path, candidate
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
