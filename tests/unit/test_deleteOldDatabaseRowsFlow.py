"""Unit tests for prefect_server.deleteOldDatabaseRowsFlow."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from prefect_server.deleteOldDatabaseRowsFlow import delete_old_database_rows_flow


def _make_mock_settings(dry_run=False, db_lookup_key="imap-database"):
    mock_settings = MagicMock()
    mock_settings.delete_old_database_rows.dry_run = dry_run
    mock_settings.delete_old_database_rows.database_url_env_var_or_block_name = (
        db_lookup_key
    )
    return mock_settings


class TestDeleteOldDatabaseRowsFlow:
    @pytest.mark.asyncio
    async def test_runs_and_returns_completed_with_summary(self) -> None:
        mock_settings = _make_mock_settings()

        with (
            patch(
                "prefect_server.deleteOldDatabaseRowsFlow.AppSettings",
                return_value=mock_settings,
            ),
            patch(
                "prefect_server.deleteOldDatabaseRowsFlow._get_database_connectionstring",
                new_callable=AsyncMock,
                return_value="postgresql://localhost/test",
            ) as mock_get_db_url,
            patch(
                "prefect_server.deleteOldDatabaseRowsFlow.delete_old_rows",
                return_value=3,
            ) as mock_delete,
        ):
            result = await delete_old_database_rows_flow.fn(
                table="ace_mag_noaa", older_than_days=7
            )

        mock_get_db_url.assert_called_once()
        mock_delete.assert_called_once()
        call_args = mock_delete.call_args.args
        assert call_args[:5] == (
            "postgresql://localhost/test",
            "ace_mag_noaa",
            "id",
            7,
            False,
        )
        assert "3 row(s) were deleted" in result.message
        assert "ace_mag_noaa" in result.message

    @pytest.mark.asyncio
    async def test_returns_skipped_when_no_rows_deleted(self) -> None:
        mock_settings = _make_mock_settings()

        with (
            patch(
                "prefect_server.deleteOldDatabaseRowsFlow.AppSettings",
                return_value=mock_settings,
            ),
            patch(
                "prefect_server.deleteOldDatabaseRowsFlow._get_database_connectionstring",
                new_callable=AsyncMock,
                return_value="postgresql://localhost/test",
            ),
            patch(
                "prefect_server.deleteOldDatabaseRowsFlow.delete_old_rows",
                return_value=0,
            ),
        ):
            result = await delete_old_database_rows_flow.fn(table="ace_mag_noaa")

        assert "No rows to delete" in result.message

    @pytest.mark.asyncio
    async def test_dry_run_parameter_overrides_config_default(self) -> None:
        mock_settings = _make_mock_settings(dry_run=False)

        with (
            patch(
                "prefect_server.deleteOldDatabaseRowsFlow.AppSettings",
                return_value=mock_settings,
            ),
            patch(
                "prefect_server.deleteOldDatabaseRowsFlow._get_database_connectionstring",
                new_callable=AsyncMock,
                return_value="postgresql://localhost/test",
            ),
            patch(
                "prefect_server.deleteOldDatabaseRowsFlow.delete_old_rows",
                return_value=2,
            ) as mock_delete,
        ):
            result = await delete_old_database_rows_flow.fn(
                table="ace_mag_noaa", dry_run=True
            )

        assert mock_delete.call_args.args[4] is True  # dry_run positional argument
        assert "would be deleted" in result.message
