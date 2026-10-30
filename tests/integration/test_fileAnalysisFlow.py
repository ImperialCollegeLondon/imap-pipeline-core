"""Integration tests for the file analyser without Prefect orchestration."""

from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest

from imap_db.model import File
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    IndexByIdsRunParameters,
    IndexByPathOrModifiedDateRunParameters,
)
from imap_mag.util.Environment import Environment
from prefect_server.fileAnalyserFlow import file_analyser_flow
from tests.util.miscellaneous import DATASTORE


def _insert_file(test_database, rel_path: str, app_settings) -> File:
    file_path = app_settings.data_store / rel_path
    stem = file_path.stem
    version = int(stem.split("_v")[-1])
    date_str = stem.split("_")[-2]
    content_date = datetime(
        int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]), tzinfo=UTC
    )
    file = File.from_file(file_path, version, "HASH", content_date, app_settings)
    file.last_modified_date = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=1)
    test_database.upsert_file(file)
    return test_database.get_files(File.path == rel_path)[0]


@pytest.mark.asyncio
async def test_flow_analyses_file_by_id(
    test_database,
):
    """Smoke test: flow indexes a specific file when given its ID."""
    rel_path = "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"

    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        settings = AppSettings()  # type: ignore
        file = _insert_file(test_database, rel_path, settings)

        await file_analyser_flow.fn(
            run_parameters=IndexByIdsRunParameters(file_ids=[file.id])
        )

        idx = test_database.get_file_analysis_by_file_id(file.id)
        assert idx is not None
        assert idx.record_count == 100


@pytest.mark.asyncio
async def test_flow_analyses_by_file_path(
    test_database,
):
    """Smoke test: flow indexes a file matched by path pattern."""
    rel_path = "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv"

    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        settings = AppSettings()  # type: ignore
        file = _insert_file(test_database, rel_path, settings)

        await file_analyser_flow.fn(
            run_parameters=IndexByPathOrModifiedDateRunParameters(file_paths=[rel_path])
        )

        idx = test_database.get_file_analysis_by_file_id(file.id)
        assert idx is not None
        assert idx.record_count == 5


@pytest.fixture
def parquet_layer_file(clean_datastore, test_database):
    rel_path = (
        "calibration/layers/2026/01/imap_mag_noop-norm-layer-data_20260101_v001.parquet"
    )
    path = clean_datastore / rel_path
    path.parent.mkdir(parents=True)
    timestamps = pd.to_datetime(
        ["2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z", "2026-01-01T00:04:00Z"]
    )
    pd.DataFrame(
        {
            "time": timestamps,
            "offset_x": [10.0, None, 2000.0],
            "offset_y": [1.0, 2.0, 3.0],
            "offset_z": [4.0, 5.0, 6.0],
        }
    ).to_parquet(path)
    settings = AppSettings()  # type: ignore
    return _insert_file(test_database, rel_path, settings)


@pytest.mark.asyncio
async def test_flow_analyses_parquet_automatically(parquet_layer_file, test_database):
    await file_analyser_flow.fn(run_parameters=AutomaticRunParameters())

    result = test_database.get_file_analysis_by_file_id(parquet_layer_file.id)
    assert result is not None
    assert result.record_count == 3
    assert result.first_timestamp == datetime(2026, 1, 1, tzinfo=UTC)
    assert result.last_timestamp == datetime(2026, 1, 1, 0, 4, tzinfo=UTC)
    assert result.has_gaps is True
    assert result.max_delta_between_timestamps == timedelta(minutes=3)


@pytest.mark.asyncio
async def test_flow_analyses_parquet_by_id(parquet_layer_file, test_database):
    await file_analyser_flow.fn(
        run_parameters=IndexByIdsRunParameters(file_ids=[parquet_layer_file.id])
    )

    result = test_database.get_file_analysis_by_file_id(parquet_layer_file.id)
    assert result is not None
    assert result.record_count == 3
    assert result.has_missing_data is True
    assert result.column_stats["offset_x"]["null_count"] == 1
    assert result.missing_data_gaps[0]["column"] == "offset_x"


@pytest.mark.asyncio
async def test_flow_analyses_parquet_by_path(parquet_layer_file, test_database):
    await file_analyser_flow.fn(
        run_parameters=IndexByPathOrModifiedDateRunParameters(
            file_paths=[parquet_layer_file.path]
        )
    )

    result = test_database.get_file_analysis_by_file_id(parquet_layer_file.id)
    assert result is not None
    assert result.record_count == 3
    assert result.has_bad_data is True
    assert result.column_stats["offset_x"]["bad_data_count"] == 1
    assert result.column_stats["offset_x"]["out_of_range_count"] == 1
