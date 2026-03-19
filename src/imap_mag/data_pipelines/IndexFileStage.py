import fnmatch
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from imap_db.model import FileIndex
from imap_mag.config.AppSettings import AppSettings
from imap_mag.config.FileIndexConfig import FileIndexPatternConfig
from imap_mag.data_pipelines import Stage
from imap_mag.data_pipelines.Record import Record


class IndexFileStage(Stage):
    """Stage that processes each file and builds a FileIndex object.

    Handles both CDF and CSV files, extracting:
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
        existing_file_index: FileIndex | None = getattr(item, "file_index", None)

        self.logger.info(f"Indexing file: {file_path_relative}")

        try:
            file_index = self._index_file(
                file_id, file_path, file_path_relative, existing_file_index
            )
        except Exception as e:
            self.logger.error(
                f"Failed to index file {file_path_relative}: {e}", exc_info=e
            )
            # Best-effort: create minimal file index on failure
            file_index = FileIndex(
                file_id=file_id,
                indexed_date=datetime.now(tz=UTC),
            )
            if existing_file_index is not None and existing_file_index.id is not None:
                file_index.id = existing_file_index.id

        await self.publish_next(
            Record(file_id=file_id, file_index=file_index),
            context=context,
        )

    def _index_file(
        self,
        file_id: int,
        file_path: Path,
        file_path_relative: str,
        existing_file_index: FileIndex | None = None,
    ) -> FileIndex:
        """Index a file and return a FileIndex object.

        If ``existing_file_index`` is supplied the returned record carries the
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
        elif suffix == ".csv":
            result = self._index_csv_file(
                file_id, file_path, file_path_relative, pattern_config
            )
        else:
            self.logger.warning(
                f"Unsupported file type: {suffix} for {file_path_relative}"
            )
            result = FileIndex(
                file_id=file_id,
                indexed_date=datetime.now(tz=UTC),
            )

        # Preserve the existing primary key so the database save is an update
        if existing_file_index is not None and existing_file_index.id is not None:
            result.id = existing_file_index.id

        return result

    def _find_pattern_config(
        self, file_path_relative: str
    ) -> FileIndexPatternConfig | None:
        """Find the matching FileIndexPatternConfig for a file path."""
        for pattern_config in self.settings.file_index.file_patterns:
            if fnmatch.fnmatch(file_path_relative, pattern_config.pattern):
                return pattern_config
        return None

    def _index_cdf_file(
        self,
        file_id: int,
        file_path: Path,
        file_path_relative: str,
        pattern_config: FileIndexPatternConfig | None,
    ) -> FileIndex:
        """Index a CDF file."""
        import cdflib

        with cdflib.CDF(str(file_path)) as cdf:
            info = cdf.cdf_info()
            variables = info.zVariables

            # Find the datetime column
            datetime_col = self._find_datetime_column_cdf(variables, pattern_config)

            timestamps = None
            record_count = None
            first_timestamp = None
            last_timestamp = None

            if datetime_col:
                try:
                    epoch_data = cdf.varget(datetime_col)
                    if epoch_data is not None and len(epoch_data) > 0:
                        record_count = len(epoch_data)
                        # Convert CDF epoch to datetime
                        times = cdflib.cdfepoch.to_datetime(epoch_data)
                        timestamps = pd.to_datetime([str(t) for t in times], utc=True)
                        del epoch_data, times  # free large intermediate arrays
                        if len(timestamps) > 0:
                            first_timestamp = timestamps[0].to_pydatetime()
                            last_timestamp = timestamps[-1].to_pydatetime()
                        self.logger.info(
                            f"CDF file {file_path_relative}: {record_count} records, "
                            f"{first_timestamp} to {last_timestamp}"
                        )
                except Exception as e:
                    self.logger.warning(
                        f"Failed to read datetime column '{datetime_col}' from {file_path_relative}: {e}"
                    )

            # If we didn't get record count from epoch, try using variables
            if record_count is None and variables:
                try:
                    first_var_data = cdf.varget(variables[0])
                    if first_var_data is not None:
                        record_count = len(first_var_data)
                    del first_var_data
                except Exception:
                    pass

            # Calculate gaps
            gaps: list[dict] = []
            has_gaps = None
            total_time_without_gaps = None
            total_gap_duration = None
            min_delta = None
            max_delta = None
            avg_delta = None
            median_delta = None

            if timestamps is not None and len(timestamps) > 1:
                gaps, has_gaps, total_time_without_gaps, total_gap_duration = (
                    self._calculate_gaps(timestamps, pattern_config)
                )
                min_delta, max_delta, avg_delta, median_delta = (
                    self._calculate_timestamp_deltas(timestamps)
                )

            # Check for NaN / bad data
            has_bad_data = None
            nan_gaps = []

            # Check for missing data
            has_missing_data = None
            missing_data_gaps = []

            # Column stats
            column_stats = {}

            if pattern_config and pattern_config.columns_to_check:
                for col_check in pattern_config.columns_to_check:
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

                        col_array = np.asarray(col_data, dtype=float)
                        stats = self._compute_column_stats(col_array, col_check)
                        column_stats[col_name] = stats

                        if (
                            col_check.check_for_bad_data
                            and stats.get("bad_data_count", 0) > 0
                        ):
                            has_bad_data = True
                            if timestamps is not None:
                                nan_gaps_for_col = self._find_nan_gaps(
                                    timestamps, col_array, col_name
                                )
                                nan_gaps.extend(nan_gaps_for_col)

                        if col_check.check_for_empty and stats.get("null_count", 0) > 0:
                            has_missing_data = True
                            if timestamps is not None:
                                missing_gaps_for_col = self._find_missing_data_gaps(
                                    timestamps, col_array, col_name
                                )
                                missing_data_gaps.extend(missing_gaps_for_col)
                        del col_data, col_array  # free column data after processing
                    except Exception as e:
                        self.logger.warning(
                            f"Failed to check column '{col_name}' in {file_path_relative}: {e}"
                        )

            if has_bad_data is None and nan_gaps:
                has_bad_data = True
            if has_missing_data is None and missing_data_gaps:
                has_missing_data = True

            # Free timestamps - no longer needed; record whether they existed for the return value
            has_timestamps = timestamps is not None
            if timestamps is not None:
                del timestamps

            # Extract CDF global attributes
            cdf_attributes_data = None
            if pattern_config and pattern_config.cdf_attributes_to_index:
                try:
                    gattrs = cdf.globalattsget()
                    cdf_attributes_data: dict[
                        str, list[str | int | float | None] | str | int | float | None
                    ] = {}
                    for attr_name in pattern_config.cdf_attributes_to_index:
                        if attr_name in gattrs:
                            attr_val = gattrs[attr_name]
                            # Convert to serializable
                            if isinstance(attr_val, list):
                                cdf_attributes_data[attr_name] = [
                                    str(v)
                                    if not isinstance(
                                        v, str | int | float | bool | None
                                    )
                                    else v
                                    for v in attr_val
                                ]
                            else:
                                cdf_attributes_data[attr_name] = (
                                    str(attr_val)
                                    if not isinstance(
                                        attr_val, str | int | float | bool | None
                                    )
                                    else attr_val
                                )
                except Exception as e:
                    self.logger.warning(
                        f"Failed to read CDF attributes from {file_path_relative}: {e}"
                    )

            return FileIndex(
                file_id=file_id,
                indexed_date=datetime.now(tz=UTC),
                record_count=record_count,
                first_timestamp=first_timestamp,
                last_timestamp=last_timestamp,
                has_gaps=has_gaps
                if has_gaps is not None
                else (True if gaps else False if has_timestamps else None),
                has_missing_data=has_missing_data,
                has_bad_data=has_bad_data,
                total_time_without_gaps=total_time_without_gaps,
                total_gap_duration=total_gap_duration,
                min_delta_between_timestamps=min_delta,
                max_delta_between_timestamps=max_delta,
                avg_delta_between_timestamps=avg_delta,
                median_delta_between_timestamps=median_delta,
                gaps=gaps if gaps else None,
                nan_gaps=nan_gaps if nan_gaps else None,
                missing_data_gaps=missing_data_gaps if missing_data_gaps else None,
                cdf_attributes=cdf_attributes_data,
                column_stats=column_stats if column_stats else None,
            )

    def _index_csv_file(
        self,
        file_id: int,
        file_path: Path,
        file_path_relative: str,
        pattern_config: FileIndexPatternConfig | None,
    ) -> FileIndex:
        """Index a CSV file."""
        df = pd.read_csv(file_path)
        record_count = len(df)

        # Find datetime column
        datetime_col = self._find_datetime_column_csv(
            df.columns.tolist(), pattern_config
        )

        timestamps = None
        first_timestamp = None
        last_timestamp = None

        if datetime_col:
            try:
                # For "epoch" columns, try CDF epoch conversion first (TT2000/CDF_EPOCH format)
                if datetime_col.lower() == "epoch":
                    try:
                        import cdflib

                        epoch_values = df[datetime_col].to_numpy()
                        times = cdflib.cdfepoch.to_datetime(epoch_values)
                        ts_series = pd.Series(
                            pd.to_datetime([str(t) for t in times], utc=True)
                        )
                    except Exception as cdf_err:
                        self.logger.debug(
                            f"CDF epoch conversion failed for '{datetime_col}' in "
                            f"{file_path_relative}: {cdf_err}. "
                            "Falling back to pandas datetime parsing."
                        )
                        ts_series = pd.to_datetime(
                            df[datetime_col], utc=True, errors="coerce"
                        )
                else:
                    ts_series = pd.to_datetime(
                        df[datetime_col], utc=True, errors="coerce"
                    )
                ts_series = ts_series.dropna()
                if len(ts_series) > 0:
                    timestamps = ts_series
                    first_timestamp = timestamps.iloc[0].to_pydatetime()
                    last_timestamp = timestamps.iloc[-1].to_pydatetime()
                    self.logger.info(
                        f"CSV file {file_path_relative}: {record_count} records, "
                        f"{first_timestamp} to {last_timestamp}"
                    )
            except Exception as e:
                self.logger.warning(
                    f"Failed to parse datetime column '{datetime_col}' from {file_path_relative}: {e}"
                )

        # Calculate gaps
        gaps = []
        has_gaps = None
        total_time_without_gaps = None
        total_gap_duration = None
        min_delta = None
        max_delta = None
        avg_delta = None
        median_delta = None

        if timestamps is not None and len(timestamps) > 1:
            gaps, has_gaps, total_time_without_gaps, total_gap_duration = (
                self._calculate_gaps(timestamps, pattern_config)
            )
            min_delta, max_delta, avg_delta, median_delta = (
                self._calculate_timestamp_deltas(timestamps)
            )

        # Check for bad data and missing data
        has_bad_data = None
        nan_gaps = []
        has_missing_data = None
        missing_data_gaps = []
        column_stats = {}

        columns_to_check = pattern_config.columns_to_check if pattern_config else []

        for col_check in columns_to_check:
            col_name = col_check.column_name
            if col_name not in df.columns:
                self.logger.debug(
                    f"Column '{col_name}' not found in CSV for {file_path_relative}"
                )
                continue
            try:
                col_series = df[col_name].copy()

                # Flag string sentinels as bad data BEFORE numeric coercion so
                # values like "NA" / "-" are not silently absorbed into missing data.
                string_bad_mask = pd.Series(False, index=col_series.index)
                if col_check.check_for_bad_data and col_check.match_as_bad_data:
                    str_series = col_series.astype(str).str.strip()
                    for sentinel in col_check.match_as_bad_data:
                        string_bad_mask |= str_series == sentinel
                    if string_bad_mask.any():
                        has_bad_data = True
                        # Replace string sentinels with NaN so numeric coercion is clean
                        col_series = col_series.where(~string_bad_mask, other=np.nan)

                # Try numeric conversion for remaining NaN/sentinel checks
                try:
                    col_array = pd.to_numeric(col_series, errors="coerce").to_numpy(
                        dtype=float
                    )
                    has_numeric = True
                except Exception:
                    col_array = None
                    has_numeric = False

                stats: dict[str, int | float] = {}
                if has_numeric and col_array is not None:
                    stats = self._compute_column_stats(col_array, col_check)
                    # Add string-sentinel bad count on top of numeric sentinel count
                    stats["bad_data_count"] = int(stats.get("bad_data_count", 0)) + int(
                        string_bad_mask.sum()
                    )
                else:
                    stats["null_count"] = int(col_series.isna().sum())
                    stats["bad_data_count"] = int(string_bad_mask.sum())

                column_stats[col_name] = stats

                if col_check.check_for_bad_data and has_numeric:
                    bad_count = stats.get("bad_data_count", 0)
                    if bad_count > 0:
                        has_bad_data = True
                        if timestamps is not None and col_array is not None:
                            nan_gaps_for_col = self._find_nan_gaps(
                                timestamps, col_array, col_name
                            )
                            nan_gaps.extend(nan_gaps_for_col)

                if col_check.check_for_empty and stats.get("null_count", 0) > 0:
                    has_missing_data = True
                    if timestamps is not None and col_array is not None:
                        missing_gaps_for_col = self._find_missing_data_gaps(
                            timestamps, col_array, col_name
                        )
                        missing_data_gaps.extend(missing_gaps_for_col)
                del col_series, col_array  # free column data after processing
            except Exception as e:
                self.logger.warning(
                    f"Failed to check column '{col_name}' in {file_path_relative}: {e}"
                )

        if has_bad_data is None and nan_gaps:
            has_bad_data = True
        if has_missing_data is None and missing_data_gaps:
            has_missing_data = True

        # Free large data structures - no longer needed after column processing
        del df
        has_timestamps = timestamps is not None
        if timestamps is not None:
            del timestamps

        return FileIndex(
            file_id=file_id,
            indexed_date=datetime.now(tz=UTC),
            record_count=record_count,
            first_timestamp=first_timestamp,
            last_timestamp=last_timestamp,
            has_gaps=has_gaps
            if has_gaps is not None
            else (True if gaps else False if has_timestamps else None),
            has_missing_data=has_missing_data,
            has_bad_data=has_bad_data,
            total_time_without_gaps=total_time_without_gaps,
            total_gap_duration=total_gap_duration,
            min_delta_between_timestamps=min_delta,
            max_delta_between_timestamps=max_delta,
            avg_delta_between_timestamps=avg_delta,
            median_delta_between_timestamps=median_delta,
            gaps=gaps if gaps else None,
            nan_gaps=nan_gaps if nan_gaps else None,
            missing_data_gaps=missing_data_gaps if missing_data_gaps else None,
            cdf_attributes=None,
            column_stats=column_stats if column_stats else None,
        )

    def _find_datetime_column_cdf(
        self, variables: list[str], pattern_config: FileIndexPatternConfig | None
    ) -> str | None:
        """Find the datetime column in CDF variables."""
        if pattern_config and pattern_config.datetime_column:
            # Check case-insensitively
            for var in variables:
                if var.lower() == pattern_config.datetime_column.lower():
                    return var
            return pattern_config.datetime_column

        # Auto-detect
        keywords = ["epoch", "time", "timestamp", "date"]
        for kw in keywords:
            for var in variables:
                if kw in var.lower():
                    return var
        return None

    def _find_datetime_column_csv(
        self, columns: list[str], pattern_config: FileIndexPatternConfig | None
    ) -> str | None:
        """Find the datetime column in CSV columns."""
        if pattern_config and pattern_config.datetime_column:
            for col in columns:
                if col.lower() == pattern_config.datetime_column.lower():
                    return col
            return pattern_config.datetime_column

        # Auto-detect
        keywords = ["epoch", "time", "timestamp", "date"]
        for kw in keywords:
            for col in columns:
                if kw in col.lower():
                    return col
        return None

    def _calculate_gaps(
        self,
        timestamps: pd.Series,
        pattern_config: FileIndexPatternConfig | None,
    ) -> tuple[list[dict], bool, timedelta | None, timedelta | None]:
        """Calculate time gaps between timestamps.

        Returns:
            Tuple of (gaps list, has_gaps bool, total_time_without_gaps, total_gap_duration)
        """
        gap_threshold_seconds: float = float(
            self.settings.file_index.default_gap_threshold_seconds
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
                gap_threshold = expected_interval * 2
                gap_threshold_seconds = gap_threshold.total_seconds()
            except Exception as e:
                self.logger.warning(
                    f"Failed to parse expected_time_between_records '{pattern_config.expected_time_between_records}': {e}"
                )

        threshold = timedelta(seconds=gap_threshold_seconds)
        ts_series = pd.Series(timestamps.values)
        diffs = ts_series.diff().dropna()

        gaps = []
        total_gap_duration = timedelta(0)

        for i, diff in enumerate(diffs, start=1):
            if diff > threshold:
                gap_start = ts_series.iloc[i - 1]
                gap_end = ts_series.iloc[i]
                duration_seconds = diff.total_seconds()
                gaps.append(
                    {
                        "start_time": pd.Timestamp(gap_start).isoformat(),
                        "end_time": pd.Timestamp(gap_end).isoformat(),
                        "duration_seconds": duration_seconds,
                    }
                )
                total_gap_duration += diff

        has_gaps = len(gaps) > 0

        # Total time without gaps
        if len(ts_series) > 1:
            total_span = ts_series.iloc[-1] - ts_series.iloc[0]
            total_time_without_gaps = total_span - total_gap_duration
        else:
            total_time_without_gaps = timedelta(0)
            total_gap_duration = timedelta(0)

        return (
            gaps,
            has_gaps,
            total_time_without_gaps,
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

        ts_series = pd.Series(timestamps.values)
        diffs = ts_series.diff().dropna()
        return (
            diffs.min().to_pytimedelta(),
            diffs.max().to_pytimedelta(),
            diffs.mean().to_pytimedelta(),
            diffs.median().to_pytimedelta(),
        )

    def _compute_column_stats(self, data: np.ndarray, col_check) -> dict:
        """Compute statistics for a column array."""
        nan_sentinel = self.settings.file_index.nan_sentinel
        stats: dict[str, int | float] = {}

        # Count nulls (actual NaN in the array)
        null_mask = np.isnan(data)
        stats["null_count"] = int(null_mask.sum())
        stats["total_count"] = len(data)

        # Count bad data (sentinel values)
        bad_mask = np.zeros(len(data), dtype=bool)
        if col_check.check_for_bad_data:
            # Check for sentinel value
            with np.errstate(invalid="ignore"):
                sentinel_mask = np.abs(data - nan_sentinel) / abs(nan_sentinel) < 1e-3
            bad_mask = bad_mask | sentinel_mask | (data < -1e29)

        stats["bad_data_count"] = int(bad_mask.sum())

        # Check range
        if col_check.expected_range is not None:
            valid_mask = ~bad_mask & ~null_mask
            if valid_mask.any():
                valid_data = data[valid_mask]
                out_of_range = (valid_data < col_check.expected_range[0]) | (
                    valid_data > col_check.expected_range[1]
                )
                stats["out_of_range_count"] = int(out_of_range.sum())
                if stats["out_of_range_count"] > 0:
                    stats["bad_data_count"] = (
                        stats["bad_data_count"] + stats["out_of_range_count"]
                    )

        # Basic stats for non-bad values
        valid_mask = ~bad_mask & ~null_mask
        if valid_mask.any():
            valid_data = data[valid_mask]
            stats["min"] = float(np.nanmin(valid_data))
            stats["max"] = float(np.nanmax(valid_data))
            stats["mean"] = float(np.nanmean(valid_data))

        return stats

    def _find_nan_gaps(
        self, timestamps: pd.Series, data: np.ndarray, col_name: str
    ) -> list[dict]:
        """Find contiguous runs of NaN/bad data values."""
        nan_sentinel = self.settings.file_index.nan_sentinel
        with np.errstate(invalid="ignore"):
            is_bad = (
                np.isnan(data)
                | (np.abs(data - nan_sentinel) / abs(nan_sentinel) < 1e-3)
                | (data < -1e29)
            )

        ts_arr = pd.Series(timestamps.values)
        result = []
        in_gap = False
        gap_start_idx = 0
        count = 0

        for i, bad in enumerate(is_bad):
            if bad and not in_gap:
                in_gap = True
                gap_start_idx = i
                count = 1
            elif bad and in_gap:
                count += 1
            elif not bad and in_gap:
                in_gap = False
                result.append(
                    {
                        "column": col_name,
                        "start_time": pd.Timestamp(
                            ts_arr.iloc[gap_start_idx]
                        ).isoformat(),
                        "end_time": pd.Timestamp(ts_arr.iloc[i - 1]).isoformat(),
                        "duration_seconds": (
                            pd.Timestamp(ts_arr.iloc[i - 1])
                            - pd.Timestamp(ts_arr.iloc[gap_start_idx])
                        ).total_seconds(),
                        "count": count,
                    }
                )
                count = 0

        if in_gap:
            result.append(
                {
                    "column": col_name,
                    "start_time": pd.Timestamp(ts_arr.iloc[gap_start_idx]).isoformat(),
                    "end_time": pd.Timestamp(ts_arr.iloc[-1]).isoformat(),
                    "duration_seconds": (
                        pd.Timestamp(ts_arr.iloc[-1])
                        - pd.Timestamp(ts_arr.iloc[gap_start_idx])
                    ).total_seconds(),
                    "count": count,
                }
            )

        return result

    def _find_missing_data_gaps(
        self, timestamps: pd.Series, data: np.ndarray, col_name: str
    ) -> list[dict]:
        """Find contiguous runs of missing (NaN) data values."""
        is_missing = np.isnan(data)
        ts_arr = pd.Series(timestamps.values)
        result = []
        in_gap = False
        gap_start_idx = 0
        count = 0

        for i, missing in enumerate(is_missing):
            if missing and not in_gap:
                in_gap = True
                gap_start_idx = i
                count = 1
            elif missing and in_gap:
                count += 1
            elif not missing and in_gap:
                in_gap = False
                result.append(
                    {
                        "column": col_name,
                        "start_time": pd.Timestamp(
                            ts_arr.iloc[gap_start_idx]
                        ).isoformat(),
                        "end_time": pd.Timestamp(ts_arr.iloc[i - 1]).isoformat(),
                        "count": count,
                    }
                )
                count = 0

        if in_gap:
            result.append(
                {
                    "column": col_name,
                    "start_time": pd.Timestamp(ts_arr.iloc[gap_start_idx]).isoformat(),
                    "end_time": pd.Timestamp(ts_arr.iloc[-1]).isoformat(),
                    "count": count,
                }
            )

        return result
