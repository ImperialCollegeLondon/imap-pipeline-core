"""Unit tests for AnalyseFilesStage."""

from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from imap_mag.config.FileAnalysisConfig import (
    ColumnCheckConfig,
    FileAnalysisConfig,
    FileAnalysisPatternConfig,
)
from imap_mag.data_pipelines.AnalyseFilesStage import AnalyseFilesStage
from imap_mag.util.Environment import Environment
from tests.util.miscellaneous import DATASTORE


def _make_settings(extra_patterns: list[FileAnalysisPatternConfig] | None = None):
    """Build AppSettings pointing at the test datastore with optional extra patterns."""
    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings()  # type: ignore

    if extra_patterns:
        settings.file_analysis = FileAnalysisConfig(
            paths_to_match=settings.file_analysis.paths_to_match,
            file_patterns=extra_patterns,
            default_gap_threshold_seconds=settings.file_analysis.default_gap_threshold_seconds,
            nan_sentinel=settings.file_analysis.nan_sentinel,
        )
    return settings


def _make_stage(
    extra_patterns: list[FileAnalysisPatternConfig] | None = None,
) -> AnalyseFilesStage:
    return AnalyseFilesStage(settings=_make_settings(extra_patterns))


# ---------------------------------------------------------------------------
# CSV - basic indexing
# ---------------------------------------------------------------------------


def test_csv_record_count():
    stage = _make_stage()
    path = DATASTORE / "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv"
    result = stage._index_file(
        1, path, "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv"
    )
    assert result.record_count == 5


def test_csv_detects_epoch_datetime_column():
    stage = _make_stage()
    path = DATASTORE / "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv"
    result = stage._index_file(
        1, path, "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv"
    )
    assert result.first_timestamp is not None
    assert result.last_timestamp is not None
    assert result.first_timestamp < result.last_timestamp


def test_csv_timestamps_parsed_successfully():
    """Epoch column contains CDF TT2000 integer nanoseconds. The code converts
    them via cdflib to UTC datetimes, so the timestamps should reflect the actual
    data date (2025-11-02)."""
    stage = _make_stage()
    path = DATASTORE / "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv"
    result = stage._index_file(
        1, path, "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv"
    )
    assert result.first_timestamp is not None
    assert result.last_timestamp is not None
    assert result.first_timestamp.year == 2025
    assert result.first_timestamp.month == 11
    assert result.first_timestamp.day == 2


# ---------------------------------------------------------------------------
# CDF - basic indexing
# ---------------------------------------------------------------------------


def test_cdf_record_count():
    stage = _make_stage()
    path = (
        DATASTORE / "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )
    result = stage._index_file(
        2, path, "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )
    assert result.record_count == 100


def test_cdf_timestamps():
    stage = _make_stage()
    path = (
        DATASTORE / "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )
    result = stage._index_file(
        2, path, "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )
    assert result.first_timestamp is not None
    assert result.last_timestamp is not None
    assert result.first_timestamp.year == 2025
    assert result.first_timestamp.month == 4
    assert result.first_timestamp.day == 21


def test_cdf_hundred_vector_file():
    stage = _make_stage()
    path = (
        DATASTORE
        / "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago-hundred-vectors_20250421_v001.cdf"
    )
    result = stage._index_file(
        3,
        path,
        "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago-hundred-vectors_20250421_v001.cdf",
    )
    assert result.record_count == 100


def test_cdf_l1d_record_count():
    stage = _make_stage()
    path = DATASTORE / "science/mag/l1d/2026/02/imap_mag_l1d_norm-gse_20260222_v001.cdf"
    result = stage._index_file(
        4, path, "science/mag/l1d/2026/02/imap_mag_l1d_norm-gse_20260222_v001.cdf"
    )
    assert result.record_count == 172800


# ---------------------------------------------------------------------------
# CDF global attributes
# ---------------------------------------------------------------------------


def test_cdf_attributes_indexed_from_config():
    """Attributes listed in config are extracted from the CDF file."""
    pattern = FileAnalysisPatternConfig(
        pattern="science/mag/l1c/**/**/imap_mag_l1c_*.cdf",
        cdf_attributes_to_index=["Parents", "ground_software_version"],
    )
    stage = _make_stage(extra_patterns=[pattern])
    path = (
        DATASTORE / "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )
    result = stage._index_file(
        2, path, "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )

    assert result.cdf_attributes is not None
    assert "Parents" in result.cdf_attributes
    assert "ground_software_version" in result.cdf_attributes
    # Parents is a list in the test file
    assert isinstance(result.cdf_attributes["Parents"], list)


def test_cdf_attributes_missing_attr_skipped():
    """An attribute that does not exist in the file is silently ignored."""
    pattern = FileAnalysisPatternConfig(
        pattern="science/mag/l1c/**/**/imap_mag_l1c_*.cdf",
        cdf_attributes_to_index=["NonExistentAttribute"],
    )
    stage = _make_stage(extra_patterns=[pattern])
    path = (
        DATASTORE / "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )
    result = stage._index_file(
        2, path, "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )

    # Dict exists but the missing key is simply absent
    assert result.cdf_attributes is not None
    assert "NonExistentAttribute" not in result.cdf_attributes


# ---------------------------------------------------------------------------
# Bad data detection (NaN sentinel)
# ---------------------------------------------------------------------------


def test_cdf_l1d_detects_bad_data():
    """L1D file has magnitude values equal to the NaN fill sentinel."""
    stage = _make_stage()
    path = DATASTORE / "science/mag/l1d/2026/02/imap_mag_l1d_norm-gse_20260222_v001.cdf"
    result = stage._index_file(
        4, path, "science/mag/l1d/2026/02/imap_mag_l1d_norm-gse_20260222_v001.cdf"
    )

    assert result.has_bad_data is True
    assert result.nan_gaps is not None
    assert len(result.nan_gaps) > 0


def test_cdf_l1d_nan_gaps_have_required_fields():
    stage = _make_stage()
    path = DATASTORE / "science/mag/l1d/2026/02/imap_mag_l1d_norm-gse_20260222_v001.cdf"
    result = stage._index_file(
        4, path, "science/mag/l1d/2026/02/imap_mag_l1d_norm-gse_20260222_v001.cdf"
    )

    assert result.nan_gaps is not None
    for gap in result.nan_gaps:
        assert "column" in gap
        assert "start_time" in gap
        assert "end_time" in gap
        assert "duration_seconds" in gap
        assert "count" in gap
        assert gap["count"] > 0


def test_cdf_l1c_no_bad_data():
    """L1C file has no NaN sentinel values."""
    stage = _make_stage()
    path = (
        DATASTORE / "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )
    result = stage._index_file(
        2, path, "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )

    # has_bad_data is None (no columns_to_check configured for l1c by default pattern)
    # but nan_gaps list should be empty/None
    assert not result.nan_gaps


# ---------------------------------------------------------------------------
# _compute_column_stats unit tests
# ---------------------------------------------------------------------------


def test_compute_column_stats_counts_nulls():
    stage = _make_stage()
    col_check = ColumnCheckConfig(column_name="x", check_for_bad_data=False)
    data = np.array([1.0, 2.0, np.nan, 4.0, np.nan])
    stats = stage._compute_column_stats(data, col_check)
    assert stats["null_count"] == 2
    assert stats["total_count"] == 5


def test_compute_column_stats_counts_bad_data_sentinel():
    stage = _make_stage()
    col_check = ColumnCheckConfig(column_name="x", check_for_bad_data=True)
    sentinel = stage.settings.file_analysis.nan_sentinel
    data = np.array([1.0, sentinel, 3.0, sentinel])
    stats = stage._compute_column_stats(data, col_check)
    assert stats["bad_data_count"] == 2


def test_compute_column_stats_counts_large_negative():
    """Values < -1e29 (but not necessarily equal to sentinel) are treated as bad."""
    stage = _make_stage()
    col_check = ColumnCheckConfig(column_name="x", check_for_bad_data=True)
    data = np.array([1.0, -1.0e31, 3.0])
    stats = stage._compute_column_stats(data, col_check)
    assert stats["bad_data_count"] >= 1


def test_compute_column_stats_out_of_range():
    stage = _make_stage()
    col_check = ColumnCheckConfig(
        column_name="x", check_for_bad_data=True, expected_range=[-10.0, 10.0]
    )
    data = np.array([1.0, 5.0, 100.0, -200.0])
    stats = stage._compute_column_stats(data, col_check)
    assert stats["out_of_range_count"] == 2


def test_compute_column_stats_basic_stats():
    stage = _make_stage()
    col_check = ColumnCheckConfig(column_name="x", check_for_bad_data=False)
    data = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    stats = stage._compute_column_stats(data, col_check)
    assert stats["min"] == pytest.approx(1.0)
    assert stats["max"] == pytest.approx(5.0)
    assert stats["mean"] == pytest.approx(3.0)


def test_compute_column_stats_all_bad_no_basic_stats():
    """When all values are bad, min/max/mean should not be present."""
    stage = _make_stage()
    sentinel = stage.settings.file_analysis.nan_sentinel
    col_check = ColumnCheckConfig(column_name="x", check_for_bad_data=True)
    data = np.full(5, sentinel)
    stats = stage._compute_column_stats(data, col_check)
    assert "min" not in stats
    assert "max" not in stats


# ---------------------------------------------------------------------------
# _find_nan_gaps unit tests
# ---------------------------------------------------------------------------


def _make_timestamps(n: int, start: datetime, step_seconds: float) -> pd.Series:
    """Create a uniform timestamp series."""
    ts = [start + timedelta(seconds=i * step_seconds) for i in range(n)]
    return pd.Series(pd.to_datetime(ts, utc=True))


def test_find_nan_gaps_single_run():
    stage = _make_stage()
    sentinel = stage.settings.file_analysis.nan_sentinel
    timestamps = _make_timestamps(10, datetime(2026, 1, 1, tzinfo=UTC), 1.0)
    data = np.array([1.0, 1.0, sentinel, sentinel, sentinel, 1.0, 1.0, 1.0, 1.0, 1.0])
    gaps = stage._find_nan_gaps(timestamps, data, "col")
    assert len(gaps) == 1
    assert gaps[0]["count"] == 3
    assert gaps[0]["column"] == "col"


def test_find_nan_gaps_two_separate_runs():
    stage = _make_stage()
    sentinel = stage.settings.file_analysis.nan_sentinel
    timestamps = _make_timestamps(10, datetime(2026, 1, 1, tzinfo=UTC), 1.0)
    data = np.array([sentinel, sentinel, 1.0, 1.0, 1.0, sentinel, 1.0, 1.0, 1.0, 1.0])
    gaps = stage._find_nan_gaps(timestamps, data, "col")
    assert len(gaps) == 2
    assert gaps[0]["count"] == 2
    assert gaps[1]["count"] == 1


def test_find_nan_gaps_trailing_run():
    """A NaN run that extends to the end of the series is captured."""
    stage = _make_stage()
    sentinel = stage.settings.file_analysis.nan_sentinel
    timestamps = _make_timestamps(6, datetime(2026, 1, 1, tzinfo=UTC), 1.0)
    data = np.array([1.0, 1.0, 1.0, sentinel, sentinel, sentinel])
    gaps = stage._find_nan_gaps(timestamps, data, "col")
    assert len(gaps) == 1
    assert gaps[0]["count"] == 3


def test_find_nan_gaps_no_bad_data():
    stage = _make_stage()
    timestamps = _make_timestamps(5, datetime(2026, 1, 1, tzinfo=UTC), 1.0)
    data = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    gaps = stage._find_nan_gaps(timestamps, data, "col")
    assert gaps == []


def test_find_nan_gaps_duration_seconds():
    """Duration of a NaN run should equal (end_time - start_time) in seconds."""
    stage = _make_stage()
    sentinel = stage.settings.file_analysis.nan_sentinel
    timestamps = _make_timestamps(5, datetime(2026, 1, 1, tzinfo=UTC), 2.0)
    data = np.array([1.0, sentinel, sentinel, sentinel, 1.0])
    gaps = stage._find_nan_gaps(timestamps, data, "col")
    assert len(gaps) == 1
    # start=index1 (t=2s), end=index3 (t=6s) → duration=4s
    assert gaps[0]["duration_seconds"] == pytest.approx(4.0)


# ---------------------------------------------------------------------------
# _find_missing_data_gaps unit tests
# ---------------------------------------------------------------------------


def test_find_missing_data_gaps_single_run():
    stage = _make_stage()
    timestamps = _make_timestamps(8, datetime(2026, 1, 1, tzinfo=UTC), 1.0)
    data = np.array([1.0, np.nan, np.nan, 1.0, 1.0, 1.0, 1.0, 1.0])
    gaps = stage._find_missing_data_gaps(timestamps, data, "col")
    assert len(gaps) == 1
    assert gaps[0]["count"] == 2


def test_find_missing_data_gaps_no_missing():
    stage = _make_stage()
    timestamps = _make_timestamps(5, datetime(2026, 1, 1, tzinfo=UTC), 1.0)
    data = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    gaps = stage._find_missing_data_gaps(timestamps, data, "col")
    assert gaps == []


def test_find_missing_data_gaps_trailing():
    stage = _make_stage()
    timestamps = _make_timestamps(5, datetime(2026, 1, 1, tzinfo=UTC), 1.0)
    data = np.array([1.0, 1.0, np.nan, np.nan, np.nan])
    gaps = stage._find_missing_data_gaps(timestamps, data, "col")
    assert len(gaps) == 1
    assert gaps[0]["count"] == 3


# ---------------------------------------------------------------------------
# _calculate_gaps unit tests
# ---------------------------------------------------------------------------


def test_calculate_gaps_no_gaps_within_threshold():
    stage = _make_stage()
    # 1-second intervals, threshold=60s → no gaps
    timestamps = _make_timestamps(10, datetime(2026, 1, 1, tzinfo=UTC), 1.0)
    gaps, has_gaps, _, duration = stage._calculate_gaps(timestamps, None)
    assert gaps == []
    assert has_gaps is False
    assert duration == timedelta(0)  # zero, not None, when there are no gaps


def test_calculate_gaps_detects_gap_above_threshold():
    stage = _make_stage()
    # Timestamps: 0, 1, 2, 3, 103s (gap of 100s > 60s default threshold)
    base = datetime(2026, 1, 1, tzinfo=UTC)
    ts_list = [base + timedelta(seconds=i) for i in [0, 1, 2, 3, 103]]
    timestamps = pd.Series(pd.to_datetime(ts_list, utc=True))
    gaps, has_gaps, _, duration = stage._calculate_gaps(timestamps, None)
    assert has_gaps is True
    assert len(gaps) == 1
    assert gaps[0]["duration_seconds"] == pytest.approx(100.0)
    assert duration is not None
    assert duration.total_seconds() == pytest.approx(100.0)


def test_calculate_gaps_custom_threshold_from_pattern_config():
    stage = _make_stage()
    # expected_time_between_records="00:00:04" → threshold = 8s
    pattern = FileAnalysisPatternConfig(
        pattern="*", expected_time_between_records="00:00:04"
    )
    base = datetime(2026, 1, 1, tzinfo=UTC)
    # Gap of 9s between index 2 and 3: above 8s threshold
    ts_list = [base + timedelta(seconds=i) for i in [0, 4, 8, 17]]
    timestamps = pd.Series(pd.to_datetime(ts_list, utc=True))
    gaps, has_gaps, _, _ = stage._calculate_gaps(timestamps, pattern)
    assert has_gaps is True
    assert len(gaps) == 1
    assert gaps[0]["duration_seconds"] == pytest.approx(9.0)


def test_calculate_gaps_subsecond_threshold():
    """0.5-second threshold from expected_time_between_records "00:00:00.5"."""
    stage = _make_stage()
    pattern = FileAnalysisPatternConfig(
        pattern="*", expected_time_between_records="00:00:00.5"
    )
    base = datetime(2026, 1, 1, tzinfo=UTC)
    ts_list = [base + timedelta(seconds=i * 0.5) for i in range(5)]
    # All gaps are 0.5s = exactly threshold (threshold = 2 * 0.5 = 1s)
    timestamps = pd.Series(pd.to_datetime(ts_list, utc=True))
    _, has_gaps, _, _ = stage._calculate_gaps(timestamps, pattern)
    assert has_gaps is False


def test_calculate_gaps_total_time_without_gaps():
    stage = _make_stage()
    base = datetime(2026, 1, 1, tzinfo=UTC)
    # Span: 105s total.  One gap of 100s (t=3 to t=103).  Without-gap time = 5s.
    ts_list = [base + timedelta(seconds=i) for i in [0, 1, 2, 3, 103, 104, 105]]
    timestamps = pd.Series(pd.to_datetime(ts_list, utc=True))
    _, _, without, duration = stage._calculate_gaps(timestamps, None)
    assert duration is not None
    assert duration.total_seconds() == pytest.approx(100.0)
    assert without is not None
    # Total span = 105s, gap = 100s → without = 5s
    assert without.total_seconds() == pytest.approx(5.0)


def test_calculate_gaps_multiple_gaps():
    stage = _make_stage()
    base = datetime(2026, 1, 1, tzinfo=UTC)
    # Two gaps: 100s and 200s
    ts_list = [base + timedelta(seconds=i) for i in [0, 1, 101, 102, 302, 303]]
    timestamps = pd.Series(pd.to_datetime(ts_list, utc=True))
    gaps, has_gaps, _, duration = stage._calculate_gaps(timestamps, None)
    assert has_gaps is True
    assert len(gaps) == 2
    assert duration is not None
    assert duration.total_seconds() == pytest.approx(300.0)


# ---------------------------------------------------------------------------
# _calculate_timestamp_deltas unit tests
# ---------------------------------------------------------------------------


def test_calculate_timestamp_deltas_uniform_intervals():
    """Uniform 1-second spacing: min=max=avg=median=1s."""
    stage = _make_stage()
    timestamps = _make_timestamps(5, datetime(2026, 1, 1, tzinfo=UTC), 1.0)
    min_d, max_d, avg_d, median_d = stage._calculate_timestamp_deltas(timestamps)
    assert min_d == timedelta(seconds=1)
    assert max_d == timedelta(seconds=1)
    assert abs(avg_d.total_seconds() - 1.0) < 1e-6
    assert abs(median_d.total_seconds() - 1.0) < 1e-6


def test_calculate_timestamp_deltas_variable_intervals():
    """Variable spacing: 1s, 1s, 10s, 1s → min=1s, max=10s, avg=3.25s, median=1s."""
    stage = _make_stage()
    base = datetime(2026, 1, 1, tzinfo=UTC)
    ts_list = [base + timedelta(seconds=i) for i in [0, 1, 2, 12, 13]]
    timestamps = pd.Series(pd.to_datetime(ts_list, utc=True))
    min_d, max_d, avg_d, median_d = stage._calculate_timestamp_deltas(timestamps)
    assert min_d == timedelta(seconds=1)
    assert max_d == timedelta(seconds=10)
    assert abs(avg_d.total_seconds() - 3.25) < 1e-6
    assert abs(median_d.total_seconds() - 1.0) < 1e-6


def test_calculate_timestamp_deltas_single_record():
    """Single timestamp: all deltas are None."""
    stage = _make_stage()
    timestamps = _make_timestamps(1, datetime(2026, 1, 1, tzinfo=UTC), 1.0)
    result = stage._calculate_timestamp_deltas(timestamps)
    assert result == (None, None, None, None)


def test_calculate_timestamp_deltas_returned_in_file_analysis():
    """CDF file indexing populates min/max/avg/median delta fields."""
    stage = _make_stage()
    path = (
        DATASTORE / "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )
    result = stage._index_file(
        2, path, "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )
    assert result.min_delta_between_timestamps is not None
    assert result.max_delta_between_timestamps is not None
    assert result.avg_delta_between_timestamps is not None
    assert result.median_delta_between_timestamps is not None
    assert result.min_delta_between_timestamps <= result.max_delta_between_timestamps


# ---------------------------------------------------------------------------
# CSV epoch conversion
# ---------------------------------------------------------------------------


def test_csv_epoch_column_uses_cdf_converter(tmp_path):
    """A CSV with an 'epoch' column containing TT2000 integers is parsed as UTC via cdflib."""
    import cdflib

    # Compute TT2000 values for two known moments 1 second apart
    t1 = cdflib.cdfepoch.compute_tt2000([2025, 4, 21, 0, 0, 0, 0, 0, 0])
    t2 = cdflib.cdfepoch.compute_tt2000([2025, 4, 21, 0, 0, 1, 0, 0, 0])

    csv_path = tmp_path / "test_epoch.csv"
    csv_path.write_text(f"epoch,value\n{t1},1.0\n{t2},2.0\n")

    stage = _make_stage()
    result = stage._index_file(1, csv_path, "test_epoch.csv")

    assert result.first_timestamp is not None
    assert result.first_timestamp.year == 2025
    assert result.first_timestamp.month == 4
    assert result.first_timestamp.day == 21
    assert result.last_timestamp is not None
    # 1-second gap between the two records
    assert (
        abs((result.last_timestamp - result.first_timestamp).total_seconds() - 1.0)
        < 0.01
    )


def test_csv_non_epoch_column_uses_pandas_parser(tmp_path):
    """A CSV with a non-epoch datetime column is still parsed by pandas, not cdflib."""
    csv_path = tmp_path / "test_time.csv"
    csv_path.write_text(
        "timestamp,value\n2025-04-21T00:00:00Z,1.0\n2025-04-21T00:00:01Z,2.0\n"
    )

    stage = _make_stage()
    result = stage._index_file(1, csv_path, "test_time.csv")

    assert result.first_timestamp is not None
    assert result.first_timestamp.year == 2025


# ---------------------------------------------------------------------------
# Best-effort: unsupported file type
# ---------------------------------------------------------------------------


def test_unsupported_file_type_returns_minimal_index(tmp_path):
    stage = _make_stage()
    fake = tmp_path / "file.parquet"
    fake.write_bytes(b"not real data")
    result = stage._index_file(99, fake, "file.parquet")
    assert result.file_id == 99
    assert result.record_count is None


@pytest.mark.asyncio
async def test_process_returns_minimal_index_on_csv_parse_error(tmp_path):
    """process() must not raise even when the file cannot be parsed.
    The outer try/except in process() catches the error and produces a minimal FileAnalysis."""

    stage = _make_stage()
    # Wire a minimal next-stage to collect what process() publishes
    collected: list = []

    class _Sink:
        async def process(self, item, context, **kwargs):
            collected.append(item)

    stage._next_stage = _Sink()  # type: ignore[assignment]

    bad = tmp_path / "bad.csv"
    bad.write_bytes(b"\x00\x01\x02\x03 not valid csv at all \x00")

    record = __import__("imap_mag.data_pipelines.Record", fromlist=["Record"]).Record(
        file_id=99,
        file_path=bad,
        file_path_relative="bad.csv",
        last_modified_date=None,
    )

    await stage.process(record, context={})

    assert len(collected) == 1
    fi = collected[0].file_analysis
    assert fi.file_id == 99
    # record_count may be None or an integer - the key guarantee is no exception was raised


# ---------------------------------------------------------------------------
# _find_pattern_config direct tests
# ---------------------------------------------------------------------------


def test_find_pattern_config_returns_none_when_no_match():
    pattern = FileAnalysisPatternConfig(pattern="science/**/*.cdf")
    stage = _make_stage(extra_patterns=[pattern])
    result = stage._find_pattern_config("hk/mag/l1/hsk-pw/2025/11/file.csv")
    assert result is None


def test_find_pattern_config_returns_first_matching_pattern():
    first = FileAnalysisPatternConfig(
        pattern="science/**/*.cdf", datetime_column="Epoch"
    )
    second = FileAnalysisPatternConfig(
        pattern="science/**/*.cdf", datetime_column="Time"
    )
    stage = _make_stage(extra_patterns=[first, second])
    result = stage._find_pattern_config("science/mag/l1c/2025/04/file.cdf")
    assert result is first


def test_find_pattern_config_matches_glob_wildcard():
    pattern = FileAnalysisPatternConfig(pattern="hk/**/*.csv")
    stage = _make_stage(extra_patterns=[pattern])
    result = stage._find_pattern_config(
        "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv"
    )
    assert result is pattern


def test_find_pattern_config_no_match_returns_none_not_error():
    stage = _make_stage()
    result = stage._find_pattern_config("completely/unknown/path/file.xyz")
    assert result is None


# ---------------------------------------------------------------------------
# _index_file: existing FileAnalysis id preservation
# ---------------------------------------------------------------------------


def test_index_file_preserves_existing_file_analysis_id():
    from imap_db.model import FileAnalysis

    stage = _make_stage()
    path = DATASTORE / "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv"
    existing = FileAnalysis(file_id=1)
    existing.id = 42
    result = stage._index_file(
        1,
        path,
        "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv",
        existing,
    )
    assert result.id == 42


def test_index_file_without_existing_leaves_id_as_none():
    stage = _make_stage()
    path = DATASTORE / "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv"
    result = stage._index_file(
        1,
        path,
        "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv",
        None,
    )
    assert result.id is None


# ---------------------------------------------------------------------------
# _find_datetime_column_cdf direct tests
# ---------------------------------------------------------------------------


def test_find_datetime_column_cdf_uses_config_column():
    pattern = FileAnalysisPatternConfig(pattern="*", datetime_column="TIME_UTC")
    stage = _make_stage(extra_patterns=[pattern])
    result = stage._find_datetime_column_cdf(["TIME_UTC", "Magnitude"], pattern)
    assert result == "TIME_UTC"


def test_find_datetime_column_cdf_config_column_is_case_insensitive():
    pattern = FileAnalysisPatternConfig(pattern="*", datetime_column="time_utc")
    stage = _make_stage(extra_patterns=[pattern])
    result = stage._find_datetime_column_cdf(["TIME_UTC", "Magnitude"], pattern)
    assert result == "TIME_UTC"


def test_find_datetime_column_cdf_auto_detects_epoch_keyword():
    stage = _make_stage()
    result = stage._find_datetime_column_cdf(["Magnitude", "Epoch", "Quality"], None)
    assert result == "Epoch"


def test_find_datetime_column_cdf_auto_detects_time_keyword():
    stage = _make_stage()
    result = stage._find_datetime_column_cdf(["Magnitude", "Time_TAG", "Quality"], None)
    assert result == "Time_TAG"


def test_find_datetime_column_cdf_returns_none_when_no_match():
    stage = _make_stage()
    result = stage._find_datetime_column_cdf(["Magnitude", "Quality", "Flags"], None)
    assert result is None


# ---------------------------------------------------------------------------
# _find_datetime_column_csv direct tests
# ---------------------------------------------------------------------------


def test_find_datetime_column_csv_uses_config_column():
    pattern = FileAnalysisPatternConfig(pattern="*", datetime_column="timestamp")
    stage = _make_stage(extra_patterns=[pattern])
    result = stage._find_datetime_column_csv(["timestamp", "value"], pattern)
    assert result == "timestamp"


def test_find_datetime_column_csv_auto_detects_epoch_keyword():
    stage = _make_stage()
    result = stage._find_datetime_column_csv(["epoch", "value", "quality"], None)
    assert result == "epoch"


def test_find_datetime_column_csv_auto_detects_time_keyword():
    stage = _make_stage()
    result = stage._find_datetime_column_csv(["value", "time_tag", "quality"], None)
    assert result == "time_tag"


def test_find_datetime_column_csv_returns_none_when_no_match():
    stage = _make_stage()
    result = stage._find_datetime_column_csv(["value", "quality", "flags"], None)
    assert result is None


# ---------------------------------------------------------------------------
# CSV: string sentinel bad data (match_as_bad_data)
# ---------------------------------------------------------------------------


def test_csv_string_sentinel_bad_data_detected(tmp_path):
    """match_as_bad_data string sentinels are detected and flagged as bad data.

    Uses sentinel strings that pandas does NOT auto-parse as NaN (unlike 'NA')
    so the string-match path in _index_csv_file is exercised.
    """
    pattern = FileAnalysisPatternConfig(
        pattern="*.csv",
        columns_to_check=[
            ColumnCheckConfig(
                column_name="x",
                check_for_bad_data=True,
                match_as_bad_data=["FILL", "BAD"],
            )
        ],
    )
    stage = _make_stage(extra_patterns=[pattern])

    csv_path = tmp_path / "test.csv"
    csv_path.write_text("epoch,x\n100,1.0\n200,FILL\n300,BAD\n400,2.0\n")

    result = stage._index_file(1, csv_path, "test.csv")

    assert result.has_bad_data is True
    assert result.column_stats is not None
    assert result.column_stats["x"]["bad_data_count"] >= 2


# ---------------------------------------------------------------------------
# CSV: missing data (NaN) detection
# ---------------------------------------------------------------------------


def test_csv_null_column_values_detected_as_missing_data(tmp_path):
    """Empty cells in a checked CSV column are flagged as missing data."""
    pattern = FileAnalysisPatternConfig(
        pattern="*.csv",
        columns_to_check=[
            ColumnCheckConfig(
                column_name="x",
                check_for_empty=True,
                check_for_bad_data=False,
            )
        ],
    )
    stage = _make_stage(extra_patterns=[pattern])

    csv_path = tmp_path / "test.csv"
    csv_path.write_text("epoch,x\n100,1.0\n200,\n300,3.0\n")

    result = stage._index_file(1, csv_path, "test.csv")

    assert result.has_missing_data is True
    assert result.column_stats is not None
    assert result.column_stats["x"]["null_count"] >= 1
