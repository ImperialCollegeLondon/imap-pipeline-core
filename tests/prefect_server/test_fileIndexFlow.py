"""E2E tests for file_index_flow (minimal - prefer unit/pipeline tests for logic coverage)."""

from datetime import UTC, datetime, timedelta

import pytest

from imap_db.model import File
from imap_mag.util.Environment import Environment
from prefect_server.fileIndexFlow import file_index_flow
from tests.util.miscellaneous import DATASTORE
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401


def _insert_file(test_database, rel_path: str, app_settings) -> File:
    file_path = DATASTORE / rel_path
    stem = file_path.stem
    version = int(stem.split("_v")[-1])
    date_str = stem.split("_")[-2]
    content_date = datetime(
        int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]), tzinfo=UTC
    )
    file = File.from_file(file_path, version, "HASH", content_date, app_settings)
    file.last_modified_date = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=1)
    test_database.insert_file(file)
    return test_database.get_files(File.path == rel_path)[0]


@pytest.mark.asyncio
async def test_flow_indexes_file_by_id(
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

        await file_index_flow(files=[file.id])

        idx = test_database.get_file_index_by_file_id(file.id)
        assert idx is not None
        assert idx.record_count == 100


@pytest.mark.asyncio
async def test_flow_indexes_by_file_path(
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

        await file_index_flow(file_paths=[rel_path])

        idx = test_database.get_file_index_by_file_id(file.id)
        assert idx is not None
        assert idx.record_count == 3915
