from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from imap_mag.config.AppSettings import AppSettings
from imap_mag.config.DeleteOldDatabaseRowsConfig import DeleteOldDatabaseRowsConfig
from imap_mag.util import Environment
from prefect_server.deleteOldDatabaseRowsFlow import delete_old_database_rows_flow
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401


def _create_noaa_like_table(engine, table_name: str) -> None:
    with engine.begin() as connection:
        connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
        connection.execute(
            text(f'CREATE TABLE "{table_name}" (id TIMESTAMP PRIMARY KEY, value FLOAT)')
        )


def _insert_row(engine, table_name: str, row_id: datetime, value: float = 1.0) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(f'INSERT INTO "{table_name}" (id, value) VALUES (:id, :value)'),
            {"id": row_id, "value": value},
        )


def _count_rows(engine, table_name: str) -> int:
    with engine.connect() as connection:
        return connection.execute(text(f'SELECT COUNT(*) FROM "{table_name}"')).scalar()


def _patch_delete_old_database_rows_config(mocker, config: DeleteOldDatabaseRowsConfig):
    original_init = AppSettings.__init__

    def patched_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self.delete_old_database_rows = config

    mocker.patch.object(AppSettings, "__init__", patched_init)


@pytest.mark.asyncio
async def test_delete_old_database_rows_flow_deletes_only_rows_older_than_cutoff(
    capture_cli_logs,
    test_database_container,
    test_database_server_engine,
    prefect_test_fixture,  # noqa: F811
    mocker,
):
    table_name = "ace_mag_noaa"
    _create_noaa_like_table(test_database_server_engine, table_name)

    now = datetime.now(UTC).replace(tzinfo=None)
    old_row = now - timedelta(days=10)
    recent_row = now - timedelta(days=1)

    _insert_row(test_database_server_engine, table_name, old_row)
    _insert_row(test_database_server_engine, table_name, recent_row)

    _patch_delete_old_database_rows_config(
        mocker, DeleteOldDatabaseRowsConfig(dry_run=False)
    )

    target_db_url = test_database_container.get_connection_url()

    with Environment(TARGET_DATABASE_URL=target_db_url):
        await delete_old_database_rows_flow(
            table=table_name,
            older_than_days=7,
            db_env_name_or_block_name_or_block="TARGET_DATABASE_URL",
        )

    assert "1 row(s) were deleted" in capture_cli_logs.text
    assert _count_rows(test_database_server_engine, table_name) == 1


@pytest.mark.asyncio
async def test_delete_old_database_rows_flow_dry_run_does_not_delete(
    capture_cli_logs,
    test_database_container,
    test_database_server_engine,
    prefect_test_fixture,  # noqa: F811
    mocker,
):
    table_name = "ace_wind_noaa"
    _create_noaa_like_table(test_database_server_engine, table_name)

    now = datetime.now(UTC).replace(tzinfo=None)
    old_row = now - timedelta(days=10)
    _insert_row(test_database_server_engine, table_name, old_row)

    _patch_delete_old_database_rows_config(
        mocker, DeleteOldDatabaseRowsConfig(dry_run=True)
    )

    target_db_url = test_database_container.get_connection_url()

    with Environment(TARGET_DATABASE_URL=target_db_url):
        await delete_old_database_rows_flow(
            table=table_name,
            older_than_days=7,
            db_env_name_or_block_name_or_block="TARGET_DATABASE_URL",
        )

    assert "1 row(s) would be deleted" in capture_cli_logs.text
    assert _count_rows(test_database_server_engine, table_name) == 1


@pytest.mark.asyncio
async def test_delete_old_database_rows_flow_handles_missing_table_gracefully(
    capture_cli_logs,
    test_database_container,
    test_database_server_engine,
    prefect_test_fixture,  # noqa: F811
    mocker,
):
    # Table has never been created (e.g. postgres-upload has not synced it yet).
    table_name = "solar_mag_noaa"
    with test_database_server_engine.begin() as connection:
        connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))

    _patch_delete_old_database_rows_config(
        mocker, DeleteOldDatabaseRowsConfig(dry_run=False)
    )

    target_db_url = test_database_container.get_connection_url()

    with Environment(TARGET_DATABASE_URL=target_db_url):
        await delete_old_database_rows_flow(
            table=table_name,
            older_than_days=7,
            db_env_name_or_block_name_or_block="TARGET_DATABASE_URL",
        )

    assert f"Table '{table_name}' does not exist yet" in capture_cli_logs.text
    assert "No rows to delete" in capture_cli_logs.text
