"""Integration tests for imap_mag.db.DeleteOldRows, backed by a real PostgreSQL database."""

import os
from datetime import datetime

import pytest
from psycopg.errors import UndefinedColumn
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from imap_mag.config.AppSettings import AppSettings
from imap_mag.config.DeleteDatabaseRowsConfig import (
    DeleteDatabaseRowsConfig,
    DeleteRowsTask,
)
from imap_mag.db.DeleteOldRows import delete_old_rows
from imap_mag.util import DatetimeProvider, Environment

DATABASE_URL_ENV_VAR = "DELETE_OLD_ROWS_TEST_DATABASE_URL"

NOW = datetime(2026, 7, 21)


def _create_table(engine, table_name: str, rows: list[tuple[int, datetime]]) -> None:
    """Create a simple table with an integer id and a `recorded_at` timestamp column."""
    with engine.begin() as connection:
        connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
        connection.execute(
            text(
                f'CREATE TABLE "{table_name}" (id INTEGER PRIMARY KEY, recorded_at TIMESTAMP)'
            )
        )
        for row_id, recorded_at in rows:
            connection.execute(
                text(
                    f'INSERT INTO "{table_name}" (id, recorded_at) VALUES (:id, :recorded_at)'
                ),
                {"id": row_id, "recorded_at": recorded_at},
            )


def _remaining_ids(engine, table_name: str) -> set[int]:
    with engine.connect() as connection:
        result = connection.execute(text(f'SELECT id FROM "{table_name}" ORDER BY id'))
        return {row[0] for row in result}


def _make_app_settings(
    tasks: list[DeleteRowsTask],
    dry_run: bool,
    database_url_env_var_or_block_name: str = DATABASE_URL_ENV_VAR,
) -> AppSettings:
    return AppSettings(
        database_delete_rows=DeleteDatabaseRowsConfig(
            dry_run=dry_run,
            database_url_env_var_or_block_name=database_url_env_var_or_block_name,
            tasks=tasks,
        )
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
@pytest.mark.asyncio
async def test_delete_old_rows_deletes_rows_older_than_threshold_and_keeps_the_rest(
    capture_cli_logs,
    test_database_container,
    test_database_server_engine,
):
    table_name = "delete_old_rows_test_basic"
    _create_table(
        test_database_server_engine,
        table_name,
        rows=[
            (1, datetime(2026, 7, 1)),  # older than cutoff (2026-07-14) -> deleted
            (2, datetime(2026, 7, 10)),  # older than cutoff -> deleted
            (
                3,
                datetime(2026, 7, 14),
            ),  # exactly at cutoff -> kept (not strictly older)
            (4, datetime(2026, 7, 20)),  # newer than cutoff -> kept
        ],
    )

    task = DeleteRowsTask(
        name="delete-old-rows",
        table=table_name,
        datetime_column="recorded_at",
        threshold_days=7,
    )
    app_settings = _make_app_settings(tasks=[task], dry_run=False)

    with Environment(
        **{DATABASE_URL_ENV_VAR: test_database_container.get_connection_url()}
    ):
        affected = await delete_old_rows(
            app_settings, datetime_provider=DatetimeProvider(fixed_now=NOW)
        )

    assert affected == 2
    assert _remaining_ids(test_database_server_engine, table_name) == {3, 4}
    assert f"Deleted 2 row(s) from '{table_name}'" in capture_cli_logs.text


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
@pytest.mark.asyncio
async def test_delete_old_rows_dry_run_only_counts_rows_and_leaves_them_in_place(
    capture_cli_logs,
    test_database_container,
    test_database_server_engine,
):
    table_name = "delete_old_rows_test_dry_run"
    _create_table(
        test_database_server_engine,
        table_name,
        rows=[
            (1, datetime(2026, 7, 1)),
            (2, datetime(2026, 7, 20)),
        ],
    )

    task = DeleteRowsTask(
        name="delete-old-rows-dry-run",
        table=table_name,
        datetime_column="recorded_at",
        threshold_days=7,
    )
    app_settings = _make_app_settings(tasks=[task], dry_run=True)

    with Environment(
        **{DATABASE_URL_ENV_VAR: test_database_container.get_connection_url()}
    ):
        affected = await delete_old_rows(
            app_settings, datetime_provider=DatetimeProvider(fixed_now=NOW)
        )

    assert affected == 1
    assert _remaining_ids(test_database_server_engine, table_name) == {1, 2}
    assert (
        f"[DRY RUN] Would delete 1 row(s) from '{table_name}'" in capture_cli_logs.text
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
@pytest.mark.asyncio
async def test_delete_old_rows_skips_missing_table_but_still_runs_other_tasks(
    capture_cli_logs,
    test_database_container,
    test_database_server_engine,
):
    existing_table = "delete_old_rows_test_existing"
    missing_table = "delete_old_rows_test_missing"

    _create_table(
        test_database_server_engine,
        existing_table,
        rows=[(1, datetime(2026, 7, 1))],
    )
    with test_database_server_engine.begin() as connection:
        connection.execute(text(f'DROP TABLE IF EXISTS "{missing_table}"'))

    tasks = [
        DeleteRowsTask(
            name="missing",
            table=missing_table,
            datetime_column="recorded_at",
            threshold_days=7,
        ),
        DeleteRowsTask(
            name="existing",
            table=existing_table,
            datetime_column="recorded_at",
            threshold_days=7,
        ),
    ]
    app_settings = _make_app_settings(tasks=tasks, dry_run=False)

    with Environment(
        **{DATABASE_URL_ENV_VAR: test_database_container.get_connection_url()}
    ):
        affected = await delete_old_rows(
            app_settings, datetime_provider=DatetimeProvider(fixed_now=NOW)
        )

    # Only the existing table's row should have been deleted; the missing
    # table must be logged and skipped without aborting the remaining tasks.
    assert affected == 1
    assert _remaining_ids(test_database_server_engine, existing_table) == set()
    assert (
        f"Table '{missing_table}' does not exist - nothing to delete"
        in capture_cli_logs.text
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
@pytest.mark.asyncio
async def test_delete_old_rows_reraises_when_datetime_column_does_not_exist(
    test_database_container,
    test_database_server_engine,
):
    table_name = "delete_old_rows_test_bad_column"
    _create_table(
        test_database_server_engine,
        table_name,
        rows=[(1, datetime(2026, 7, 1))],
    )

    task = DeleteRowsTask(
        name="bad-column",
        table=table_name,
        datetime_column="does_not_exist",
        threshold_days=7,
    )
    app_settings = _make_app_settings(tasks=[task], dry_run=False)

    with Environment(
        **{DATABASE_URL_ENV_VAR: test_database_container.get_connection_url()}
    ):
        with pytest.raises(ProgrammingError) as exc_info:
            await delete_old_rows(
                app_settings, datetime_provider=DatetimeProvider(fixed_now=NOW)
            )

    assert isinstance(exc_info.value.orig, UndefinedColumn)
    # Row must be untouched since the statement never succeeded.
    assert _remaining_ids(test_database_server_engine, table_name) == {1}


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
@pytest.mark.asyncio
async def test_delete_old_rows_works_with_bare_postgresql_url_missing_driver(
    test_database_container,
    test_database_server_engine,
):
    # `get_database_connectionstring` strips the psycopg driver suffix so the URL
    # can be handed to crump (used by the postgres-upload flow). `delete_old_rows`
    # must restore it, or SQLAlchemy defaults to the (uninstalled) psycopg2 driver
    # and the connection fails outright.
    table_name = "delete_old_rows_test_bare_url"
    _create_table(
        test_database_server_engine,
        table_name,
        rows=[(1, datetime(2026, 7, 1))],
    )

    task = DeleteRowsTask(
        name="bare-url",
        table=table_name,
        datetime_column="recorded_at",
        threshold_days=7,
    )
    app_settings = _make_app_settings(tasks=[task], dry_run=False)

    bare_url = test_database_container.get_connection_url(driver=None)
    assert "+psycopg" not in bare_url

    with Environment(**{DATABASE_URL_ENV_VAR: bare_url}):
        affected = await delete_old_rows(
            app_settings, datetime_provider=DatetimeProvider(fixed_now=NOW)
        )

    assert affected == 1
    assert _remaining_ids(test_database_server_engine, table_name) == set()
