import fnmatch
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import cdflib
import numpy as np
import pandas as pd

from imap_db.model import FileAnalysis
from imap_mag.config.AppSettings import AppSettings
from imap_mag.config.FileAnalysisConfig import (
    ColumnCheckConfig,
    FileAnalysisPatternConfig,
)
from imap_mag.data_pipelines import Stage
from imap_mag.data_pipelines.Record import Record

# Any value below this is treated as a fill value (CDF float fill is -1e31)
LARGE_NEGATIVE_FILL_THRESHOLD = -1e29
DATETIME_COLUMN_KEYWORDS = ["epoch", "time", "timestamp", "date"]


@dataclass
class ColumnCheckResults:
    """Accumulated results of checking all configured columns in a file."""

    column_stats: dict = field(default_factory=dict)
    nan_gaps: list[dict] = field(default_factory=list)
    missing_data_gaps: list[dict] = field(default_factory=list)
    has_bad_data: bool | None = None
    has_missing_data: bool | None = None


class AnalyseFilesStage(Stage):
    """Stage that processes each file and builds a FileAnalysis object.

    Handles CDF, CSV and Parquet files, extracting:
    - Record counts
    - Timestamps (first/last)
    - Time gaps
    - Bad data detection
    - Missing data detection
    - CDF global attributes
    """

    def __init__(self, settings: AppSettings):
        super().__init__()
        self.settings = settings

    async def process(self, item: Record, context: dict, **kwargs):
        file_id = item.file_id
        file_path: Path = item.file_path
        file_path_relative: str = item.file_path_relative
        existing_file_analysis: FileAnalysis | None = getattr(
            item, "file_analysis", None
        )

        self.logger.info(f"Indexing file: {file_path_relative}")

        try:
            file_analysis = self._index_file(
                file_id, file_path, file_path_relative, existing_file_analysis
            )
        except Exception as e:
            self.logger.error(
                f"Failed to index file {file_path_relative}: {e}", exc_info=e
            )
            # Best-effort: create minimal file index on failure
            file_analysis = FileAnalysis(
                file_id=file_id,
                indexed_date=datetime.now(tz=UTC),
            )
            if (
                existing_file_analysis is not None
                and existing_file_analysis.id is not None
            ):
                file_analysis.id = existing_file_analysis.id

        await self.publish_next(
            Record(file_id=file_id, file_analysis=file_analysis),
            context=context,
        )

    def _index_file(
        self,
        file_id: int,
        file_path: Path,
        file_path_relative: str,
        existing_file_analysis: FileAnalysis | None = None,
    ) -> FileAnalysis:
        """Index a file and return a FileAnalysis object.

        If ``existing_file_analysis`` is supplied the returned record carries the
        same primary key so that ``database.save()`` updates the existing row
        rather than inserting a new one.
        """
        suffix = file_path.suffix.lower()

        # Find matching pattern config
        pattern_config = self._find_pattern_config(file_path_relative)

        if suffix == ".cdf":
            result = self._index_cdf_file(
                file_id, file_path, file_path_relative, pattern_config
            )
        elif suffix in (".csv", ".parquet"):
            result = self._index_tabular_file(
                file_id, file_path, file_path_relative, pattern_config
            )
        else:
            self.logger.warning(
                f"Unsupported file type: {suffix} for {file_path_relative}"
            )
            result = FileAnalysis(
                file_id=file_id,
                indexed_date=datetime.now(tz=UTC),
            )

        # Preserve the existing primary key so the database save is an update
        if existing_file_analysis is not None and existing_file_analysis.id is not None:
            result.id = existing_file_analysis.id

        return result

    def _find_pattern_config(
        self, file_path_relative: str
    ) -> FileAnalysisPatternConfig | None:
        """Find the matching FileAnalysisPatternConfig for a file path."""
        for pattern_config in self.settings.file_analysis.file_patterns:
            if fnmatch.fnmatch(file_path_relative, pattern_config.pattern):
                return pattern_config
        return None

    def _index_cdf_file(
        self,
        file_id: int,
        file_path: Path,
        file_path_relative: str,
        pattern_config: FileAnalysisPatternConfig | None,
    ) -> FileAnalysis:
        """Index a CDF file."""
        with cdflib.CDF(str(file_path)) as cdf:
            variables = cdf.cdf_info().zVariables
            datetime_col = self._find_datetime_column_cdf(variables, pattern_config)

            record_count, timestamps = self._read_cdf_timestamps(
                cdf, variables, datetime_col, file_path_relative
            )

            results = ColumnCheckResults()
            columns_to_check = pattern_config.columns_to_check if pattern_config else []
            for col_check in columns_to_check:
                col_name = col_check.column_name
                if col_name not in variables:
                    self.logger.debug(
                        f"Column '{col_name}' not found in CDF variables for {file_path_relative}"
                    )
                    continue
                try:
                    col_data = cdf.varget(col_name)
                    if col_data is None:
                        continue
                    fill_value = cdf.varattsget(col_name).get("FILLVAL")
                    self._check_column(
                        results,
                        np.asarray(col_data, dtype=float),
                        col_check,
                        timestamps,
                        fill_value=self._to_float_or_none(fill_value),
                    )
                    del col_data  # free column data after processing
                except Exception as e:
                    self.logger.warning(
                        f"Failed to check column '{col_name}' in {file_path_relative}: {e}"
                    )

            cdf_attributes = self._read_cdf_attributes(
                cdf, pattern_config, file_path_relative
            )

        return self._build_file_analysis(
            file_id,
            record_count,
            timestamps,
            pattern_config,
            results,
            cdf_attributes,
        )

    def _read_cdf_timestamps(
        self,
        cdf: cdflib.CDF,
        variables: list[str],
        datetime_col: str | None,
        file_path_relative: str,
    ) -> tuple[int | None, pd.DatetimeIndex | None]:
        """Read the record count and timestamps from a CDF file.

        The returned timestamps are aligned with the records in the file, so fill
        values in the datetime variable appear as NaT.
        """
        if datetime_col:
            try:
                epoch_data = cdf.varget(datetime_col)
                if epoch_data is None or len(epoch_data) == 0:
                    self.logger.info(f"CDF file {file_path_relative} has no records")
                    return 0, None

                timestamps = pd.to_datetime(
                    cdflib.cdfepoch.to_datetime(epoch_data), utc=True
                )
                self.logger.info(
                    f"CDF file {file_path_relative}: {len(timestamps)} records, "
                    f"{timestamps.min()} to {timestamps.max()}"
                )
                return len(timestamps), timestamps
            except Exception as e:
                self.logger.warning(
                    f"Failed to read datetime column '{datetime_col}' from {file_path_relative}: {e}"
                )

        # No usable datetime variable: take the record count from the CDF metadata
        for var in variables:
            try:
                var_info = cdf.varinq(var)
                if var_info.Rec_Vary:
                    return var_info.Last_Rec + 1, None
            except Exception:
                continue
        return None, None

    def _read_cdf_attributes(
        self,
        cdf: cdflib.CDF,
        pattern_config: FileAnalysisPatternConfig | None,
        file_path_relative: str,
    ) -> dict | None:
        """Extract configured CDF global attributes as JSON-serialisable values."""
        if not pattern_config or not pattern_config.cdf_attributes_to_index:
            return None

        def to_serialisable(value):
            return (
                value
                if isinstance(value, str | int | float | bool | None)
                else str(value)
            )

        try:
            gattrs = cdf.globalattsget()
        except Exception as e:
            self.logger.warning(
                f"Failed to read CDF attributes from {file_path_relative}: {e}"
            )
            return None

        cdf_attributes: dict[
            str, list[str | int | float | None] | str | int | float | None
        ] = {}
        for attr_name in pattern_config.cdf_attributes_to_index:
            if attr_name in gattrs:
                attr_val = gattrs[attr_name]
                if isinstance(attr_val, list):
                    cdf_attributes[attr_name] = [to_serialisable(v) for v in attr_val]
                else:
                    cdf_attributes[attr_name] = to_serialisable(attr_val)
        return cdf_attributes

    def _index_tabular_file(
        self,
        file_id: int,
        file_path: Path,
        file_path_relative: str,
        pattern_config: FileAnalysisPatternConfig | None,
    ) -> FileAnalysis:
        """Index a CSV or Parquet file using the same column and timing checks."""
        if file_path.suffix.lower() == ".parquet":
            df = pd.read_parquet(file_path)
        else:
            try:
                df = pd.read_csv(file_path)
            except pd.errors.EmptyDataError:
                self.logger.info(f"CSV file {file_path_relative} is empty")
                return self._build_file_analysis(
                    file_id, 0, None, pattern_config, ColumnCheckResults(), None
                )

        record_count = len(df)
        datetime_col = self._find_datetime_column_csv(
            df.columns.tolist(), pattern_config
        )
        timestamps = self._parse_csv_timestamps(df, datetime_col, file_path_relative)

        results = ColumnCheckResults()
        columns_to_check = pattern_config.columns_to_check if pattern_config else []
        for col_check in columns_to_check:
            col_name = col_check.column_name
            if col_name not in df.columns:
                self.logger.debug(
                    f"Column '{col_name}' not found in {file_path_relative}"
                )
                continue
            try:
                col_series = df[col_name]

                # Flag string sentinels as bad data BEFORE numeric coercion so
                # values like "NA" / "-" are not silently absorbed into missing data.
                string_bad_mask = np.zeros(len(col_series), dtype=bool)
                if col_check.check_for_bad_data and col_check.match_as_bad_data:
                    str_series = col_series.astype(str).str.strip()
                    string_bad_mask = str_series.isin(
                        col_check.match_as_bad_data
                    ).to_numpy()
                    # Replace string sentinels with NaN so numeric coercion is clean
                    col_series = col_series.where(~string_bad_mask, other=np.nan)

                col_array = pd.to_numeric(col_series, errors="coerce").to_numpy(
                    dtype=float
                )
                self._check_column(
                    results,
                    col_array,
                    col_check,
                    timestamps,
                    extra_bad_mask=string_bad_mask,
                )
                del col_series, col_array  # free column data after processing
            except Exception as e:
                self.logger.warning(
                    f"Failed to check column '{col_name}' in {file_path_relative}: {e}"
                )

        del df  # free large data structures

        return self._build_file_analysis(
            file_id, record_count, timestamps, pattern_config, results, None
        )

    def _parse_csv_timestamps(
        self, df: pd.DataFrame, datetime_col: str | None, file_path_relative: str
    ) -> pd.Series | None:
        """Parse a tabular datetime column into timestamps aligned with its rows.

        Unparseable values appear as NaT so that row indices still line up with
        the data columns.
        """
        if not datetime_col or datetime_col not in df.columns:
            return None

        try:
            ts_series: pd.Series | None = None
            # For "epoch" columns, try CDF epoch conversion first (TT2000/CDF_EPOCH format)
            if datetime_col.lower() == "epoch":
                try:
                    times = cdflib.cdfepoch.to_datetime(df[datetime_col].to_numpy())
                    ts_series = pd.Series(pd.to_datetime(times, utc=True))
                except Exception as cdf_err:
                    self.logger.debug(
                        f"CDF epoch conversion failed for '{datetime_col}' in "
                        f"{file_path_relative}: {cdf_err}. "
                        "Falling back to pandas datetime parsing."
                    )
            if ts_series is None:
                ts_series = pd.to_datetime(df[datetime_col], utc=True, errors="coerce")

            ts_series = ts_series.reset_index(drop=True)
            if ts_series.notna().any():
                self.logger.info(
                    f"File {file_path_relative}: {len(df)} records, "
                    f"{ts_series.min()} to {ts_series.max()}"
                )
                return ts_series
        except Exception as e:
            self.logger.warning(
                f"Failed to parse datetime column '{datetime_col}' from {file_path_relative}: {e}"
            )
        return None

    def _check_column(
        self,
        results: ColumnCheckResults,
        data: np.ndarray,
        col_check: ColumnCheckConfig,
        timestamps: pd.Series | pd.DatetimeIndex | None,
        fill_value: float | None = None,
        extra_bad_mask: np.ndarray | None = None,
    ) -> None:
        """Check one column for bad and missing data, accumulating into ``results``.

        Args:
            results: Accumulator for stats and gaps across all columns.
            data: Column values as floats, aligned with ``timestamps``.
            col_check: Configuration for which checks to apply.
            timestamps: Timestamps aligned with ``data``, used to locate gaps.
            fill_value: Fill value declared for this column (e.g. CDF FILLVAL).
            extra_bad_mask: Rows already identified as bad (e.g. string sentinels).
        """
        col_name = col_check.column_name
        stats = self._compute_column_stats(data, col_check, fill_value)
        if extra_bad_mask is not None:
            stats["bad_data_count"] = int(stats["bad_data_count"]) + int(
                extra_bad_mask.sum()
            )
        results.column_stats[col_name] = stats

        has_timestamps = timestamps is not None and len(timestamps) == len(data)

        if col_check.check_for_bad_data and stats["bad_data_count"] > 0:
            results.has_bad_data = True
            if has_timestamps:
                results.nan_gaps.extend(
                    self._find_nan_gaps(timestamps, data, col_name, fill_value)  # type: ignore[arg-type]
                )

        if col_check.check_for_empty and stats["null_count"] > 0:
            results.has_missing_data = True
            if has_timestamps:
                results.missing_data_gaps.extend(
                    self._find_missing_data_gaps(timestamps, data, col_name)  # type: ignore[arg-type]
                )

    def _build_file_analysis(
        self,
        file_id: int,
        record_count: int | None,
        timestamps: pd.Series | pd.DatetimeIndex | None,
        pattern_config: FileAnalysisPatternConfig | None,
        results: ColumnCheckResults,
        cdf_attributes: dict | None,
    ) -> FileAnalysis:
        """Compute timing statistics and assemble the FileAnalysis record."""
        valid_timestamps = (
            pd.Series(timestamps).dropna().reset_index(drop=True)
            if timestamps is not None
            else None
        )

        first_timestamp = None
        last_timestamp = None
        has_gaps = None
        gaps: list[dict] = []
        total_time_without_gaps = None
        total_gap_duration = None
        min_delta = max_delta = avg_delta = median_delta = None

        if valid_timestamps is not None and len(valid_timestamps) > 0:
            first_timestamp = valid_timestamps.iloc[0].to_pydatetime(warn=False)
            last_timestamp = valid_timestamps.iloc[-1].to_pydatetime(warn=False)
            has_gaps = False

        if valid_timestamps is not None and len(valid_timestamps) > 1:
            gaps, has_gaps, total_time_without_gaps, total_gap_duration = (
                self._calculate_gaps(valid_timestamps, pattern_config)
            )
            min_delta, max_delta, avg_delta, median_delta = (
                self._calculate_timestamp_deltas(valid_timestamps)
            )

        return FileAnalysis(
            file_id=file_id,
            indexed_date=datetime.now(tz=UTC),
            record_count=record_count,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            has_gaps=has_gaps,
            has_missing_data=results.has_missing_data,
            has_bad_data=results.has_bad_data,
            total_time_without_gaps=total_time_without_gaps,
            total_gap_duration=total_gap_duration,
            min_delta_between_timestamps=min_delta,
            max_delta_between_timestamps=max_delta,
            avg_delta_between_timestamps=avg_delta,
            median_delta_between_timestamps=median_delta,
            gaps=gaps or None,
            nan_gaps=results.nan_gaps or None,
            missing_data_gaps=results.missing_data_gaps or None,
            cdf_attributes=cdf_attributes,
            column_stats=results.column_stats or None,
        )

    def _find_datetime_column_cdf(
        self, variables: list[str], pattern_config: FileAnalysisPatternConfig | None
    ) -> str | None:
        """Find the datetime column in CDF variables."""
        return self._find_datetime_column(variables, pattern_config)

    def _find_datetime_column_csv(
        self, columns: list[str], pattern_config: FileAnalysisPatternConfig | None
    ) -> str | None:
        """Find the datetime column in CSV columns."""
        return self._find_datetime_column(columns, pattern_config)

    @staticmethod
    def _find_datetime_column(
        names: list[str], pattern_config: FileAnalysisPatternConfig | None
    ) -> str | None:
        """Find the datetime column, preferring the configured name (case-insensitive)."""
        if pattern_config and pattern_config.datetime_column:
            for name in names:
                if name.lower() == pattern_config.datetime_column.lower():
                    return name
            return pattern_config.datetime_column

        # Auto-detect
        for kw in DATETIME_COLUMN_KEYWORDS:
            for name in names:
                if kw in name.lower():
                    return name
        return None

    def _calculate_gaps(
        self,
        timestamps: pd.Series,
        pattern_config: FileAnalysisPatternConfig | None,
    ) -> tuple[list[dict], bool, timedelta | None, timedelta | None]:
        """Calculate time gaps between timestamps.

        Returns:
            Tuple of (gaps list, has_gaps bool, total_time_without_gaps, total_gap_duration)
        """
        gap_threshold_seconds: float = float(
            self.settings.file_analysis.default_gap_threshold_seconds
        )

        if pattern_config and pattern_config.expected_time_between_records:
            try:
                # Parse HH:MM:SS.s format
                time_str = pattern_config.expected_time_between_records
                parts = time_str.split(":")
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = float(parts[2]) if len(parts) > 2 else 0
                expected_interval = timedelta(
                    hours=hours, minutes=minutes, seconds=seconds
                )
                # Use 2x expected as gap threshold
                gap_threshold_seconds = (expected_interval * 2).total_seconds()
            except Exception as e:
                self.logger.warning(
                    f"Failed to parse expected_time_between_records '{pattern_config.expected_time_between_records}': {e}"
                )

        ts_values = self._to_datetime64(timestamps)
        if len(ts_values) < 2:
            return [], False, timedelta(0), timedelta(0)

        diffs = np.diff(ts_values)
        threshold = np.timedelta64(int(gap_threshold_seconds * 1e9), "ns")
        gap_indices = np.flatnonzero(diffs > threshold)

        gaps = [
            {
                "start_time": pd.Timestamp(ts_values[i]).isoformat(),
                "end_time": pd.Timestamp(ts_values[i + 1]).isoformat(),
                "duration_seconds": pd.Timedelta(diffs[i]).total_seconds(),
            }
            for i in gap_indices
        ]
        total_gap_duration = pd.Timedelta(diffs[gap_indices].sum()).to_pytimedelta()
        total_span = pd.Timedelta(ts_values[-1] - ts_values[0]).to_pytimedelta()

        return (
            gaps,
            len(gaps) > 0,
            total_span - total_gap_duration,
            total_gap_duration,
        )

    def _calculate_timestamp_deltas(
        self, timestamps: pd.Series
    ) -> tuple[timedelta | None, timedelta | None, timedelta | None, timedelta | None]:
        """Calculate min, max, average, and median deltas between consecutive timestamps.

        Args:
            timestamps: Series of timestamps (must have >= 2 elements).

        Returns:
            Tuple of (min_delta, max_delta, avg_delta, median_delta) as timedelta objects,
            or (None, None, None, None) if fewer than 2 timestamps.
        """
        if len(timestamps) < 2:
            return None, None, None, None

        diffs = pd.Series(self._to_datetime64(timestamps)).diff().dropna()
        return (
            diffs.min().to_pytimedelta(),
            diffs.max().to_pytimedelta(),
            diffs.mean().to_pytimedelta(),
            diffs.median().to_pytimedelta(),
        )

    def _bad_data_mask(
        self, data: np.ndarray, fill_value: float | None = None
    ) -> np.ndarray:
        """Return a mask of values matching the configured sentinel, the column fill value
        or any large negative fill-like value.

        A sentinel is a placeholder value written where there is no real measurement,
        so matching it means "missing", not a genuine reading.
        """
        bad_mask = data < LARGE_NEGATIVE_FILL_THRESHOLD
        for sentinel in (self.settings.file_analysis.nan_sentinel, fill_value):
            if sentinel is None or np.isnan(sentinel):
                continue
            if sentinel == 0:
                bad_mask |= data == 0
            else:
                # Compare with a relative tolerance rather than ==, because the
                # sentinel loses precision when stored as float32 so an exact
                # match would miss it
                bad_mask |= np.abs(data - sentinel) / abs(sentinel) < 1e-3
        return bad_mask

    def _compute_column_stats(
        self,
        data: np.ndarray,
        col_check: ColumnCheckConfig,
        fill_value: float | None = None,
    ) -> dict:
        """Compute statistics for a column array."""
        stats: dict[str, int | float] = {}

        with np.errstate(invalid="ignore"):
            # Count nulls (actual NaN in the array)
            null_mask = np.isnan(data)
            stats["null_count"] = int(null_mask.sum())
            stats["total_count"] = len(data)

            # Count bad data (sentinel / fill values)
            bad_mask = (
                self._bad_data_mask(data, fill_value)
                if col_check.check_for_bad_data
                else np.zeros(data.shape, dtype=bool)
            )
            stats["bad_data_count"] = int(bad_mask.sum())

            valid_mask = ~bad_mask & ~null_mask

            # Check range
            if col_check.expected_range is not None and valid_mask.any():
                valid_data = data[valid_mask]
                out_of_range = (valid_data < col_check.expected_range[0]) | (
                    valid_data > col_check.expected_range[1]
                )
                stats["out_of_range_count"] = int(out_of_range.sum())
                stats["bad_data_count"] += stats["out_of_range_count"]

            # Basic stats for non-bad values
            if valid_mask.any():
                valid_data = data[valid_mask]
                stats["min"] = float(np.min(valid_data))
                stats["max"] = float(np.max(valid_data))
                stats["mean"] = float(np.mean(valid_data))

        return stats

    def _find_nan_gaps(
        self,
        timestamps: pd.Series,
        data: np.ndarray,
        col_name: str,
        fill_value: float | None = None,
    ) -> list[dict]:
        """Find contiguous runs of NaN/bad data values."""
        with np.errstate(invalid="ignore"):
            is_bad = np.isnan(data) | self._bad_data_mask(data, fill_value)

        ts_values = self._to_datetime64(timestamps)
        result = []
        for start, end in self._find_runs(is_bad):
            start_time = pd.Timestamp(ts_values[start])
            end_time = pd.Timestamp(ts_values[end])
            result.append(
                {
                    "column": col_name,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "duration_seconds": (end_time - start_time).total_seconds(),
                    "count": end - start + 1,
                }
            )
        return result

    def _find_missing_data_gaps(
        self, timestamps: pd.Series, data: np.ndarray, col_name: str
    ) -> list[dict]:
        """Find contiguous runs of missing (NaN) data values."""
        ts_values = self._to_datetime64(timestamps)
        return [
            {
                "column": col_name,
                "start_time": pd.Timestamp(ts_values[start]).isoformat(),
                "end_time": pd.Timestamp(ts_values[end]).isoformat(),
                "count": end - start + 1,
            }
            for start, end in self._find_runs(np.isnan(data))
        ]

    @staticmethod
    def _find_runs(mask: np.ndarray) -> list[tuple[int, int]]:
        """Return (start, end) inclusive indices of each contiguous run of True values.

        Multi-dimensional masks are reduced per record, so a record is flagged if
        any of its elements are flagged.
        """
        if mask.ndim > 1:
            mask = mask.reshape(mask.shape[0], -1).any(axis=1)
        padded = np.concatenate(([False], mask, [False])).astype(np.int8)
        edges = np.diff(padded)
        starts = np.flatnonzero(edges == 1)
        ends = np.flatnonzero(edges == -1) - 1
        return list(zip(starts.tolist(), ends.tolist(), strict=True))

    @staticmethod
    def _to_datetime64(timestamps: pd.Series | pd.DatetimeIndex) -> np.ndarray:
        """Convert timestamps to a naive (UTC) numpy datetime64 array."""
        return pd.Series(timestamps).to_numpy(dtype="datetime64[ns]")

    @staticmethod
    def _to_float_or_none(value) -> float | None:
        try:
            return float(np.asarray(value).ravel()[0])
        except (TypeError, ValueError, IndexError):
            return None
