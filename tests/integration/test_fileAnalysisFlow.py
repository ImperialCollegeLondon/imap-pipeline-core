"""E2E tests for file_analyser_flow (minimal - prefer unit/pipeline tests for logic coverage)."""

from datetime import UTC, datetime, timedelta

import cdflib
import pandas as pd
import pytest

from imap_db.model import File
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    IndexByIdsRunParameters,
    IndexByPathOrModifiedDateRunParameters,
)
from imap_mag.util.Environment import Environment
from prefect_server.fileAnalyserFlow import file_analyser_flow
from tests.util.miscellaneous import DATASTORE
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401


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
    test_database_container,
    prefect_test_fixture,  # noqa: F811
):
    """Smoke test: flow indexes a specific file when given its ID."""
    rel_path = "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"

    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings()  # type: ignore
        file = _insert_file(test_database, rel_path, settings)

        await file_analyser_flow(
            run_parameters=IndexByIdsRunParameters(file_ids=[file.id])
        )

        idx = test_database.get_file_analysis_by_file_id(file.id)
        assert idx is not None
        assert idx.record_count == 100


@pytest.mark.asyncio
async def test_flow_analyses_by_file_path(
    test_database,
    test_database_container,
    prefect_test_fixture,  # noqa: F811
):
    """Smoke test: flow indexes a file matched by path pattern."""
    rel_path = "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv"

    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings()  # type: ignore
        file = _insert_file(test_database, rel_path, settings)

        await file_analyser_flow(
            run_parameters=IndexByPathOrModifiedDateRunParameters(file_paths=[rel_path])
        )

        idx = test_database.get_file_analysis_by_file_id(file.id)
        assert idx is not None
        assert idx.record_count == 5


@pytest.mark.asyncio
@pytest.mark.parametrize("instrument", ["mag", "sc", "lo"])
async def test_flow_analyses_parquet(
    instrument,
    clean_datastore,
    test_database,
    prefect_test_fixture,  # noqa: F811
):
    from imap_mag.config.AppSettings import AppSettings

    rel_path = (
        f"hk/{instrument}/l1/example/2026/01/"
        f"imap_{instrument}_l1_example_20260101_v001.parquet"
    )
    path = clean_datastore / rel_path
    path.parent.mkdir(parents=True)
    timestamps = pd.to_datetime(
        ["2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z", "2026-01-01T00:04:00Z"]
    )
    if instrument == "lo":
        data = pd.DataFrame(
            {
                "time (yyyy-MM-dd'T'HH:mm:ss.SSS)": timestamps,
                "ILOGLOBAL.PPM_NHK_POT_PRI (deg)": [10.0, None, 400.0],
            }
        )
    else:
        data = pd.DataFrame(
            {
                "epoch": [
                    int(
                        cdflib.cdfepoch.compute_tt2000(
                            [2026, 1, 1, 0, minute, 0, 0, 0, 0]
                        )
                    )
                    for minute in [0, 1, 4]
                ],
                "shcourse": [1.0, None, 3.0],
            }
        )
    data.to_parquet(path)
    settings = AppSettings()  # type: ignore
    file = _insert_file(test_database, rel_path, settings)
    if instrument == "mag":
        run_parameters = AutomaticRunParameters()
    elif instrument == "sc":
        run_parameters = IndexByIdsRunParameters(file_ids=[file.id])
    else:
        run_parameters = IndexByPathOrModifiedDateRunParameters(file_paths=[rel_path])

    await file_analyser_flow(run_parameters=run_parameters)

    result = test_database.get_file_analysis_by_file_id(file.id)
    assert result is not None
    assert result.record_count == 3
    assert result.first_timestamp == timestamps[0].to_pydatetime()
    assert result.last_timestamp == timestamps[-1].to_pydatetime()
    assert result.has_gaps is True
    assert result.max_delta_between_timestamps == timedelta(minutes=3)
    if instrument == "sc":
        assert result.has_missing_data is True
        assert result.column_stats["shcourse"]["null_count"] == 1
    if instrument == "lo":
        assert result.has_bad_data is True
        assert result.has_missing_data is True
        assert (
            result.column_stats["ILOGLOBAL.PPM_NHK_POT_PRI (deg)"]["bad_data_count"]
            == 1
        )
