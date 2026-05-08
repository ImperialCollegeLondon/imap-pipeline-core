"""Unit tests for pollSpice Prefect flow."""

from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from prefect_server.pollSpice import poll_spice_flow


def _make_mock_date_manager(start=None, end=None):
    mock_dm = MagicMock()
    mock_dm.get_dates_for_download.return_value = (
        start or date(2025, 1, 1),
        end or date(2025, 1, 31),
    )
    return mock_dm


class TestPollSpiceFlowUnit:
    @pytest.mark.asyncio
    async def test_flow_downloads_spice_with_user_provided_dates(self):
        mock_date_manager = _make_mock_date_manager()

        with (
            patch("prefect_server.pollSpice.get_run_logger", return_value=MagicMock()),
            patch(
                "prefect_server.pollSpice.Database",
                return_value=MagicMock(),
            ),
            patch(
                "prefect_server.pollSpice.DownloadDateManager",
                return_value=mock_date_manager,
            ),
            patch(
                "prefect_server.pollSpice.get_secret_or_env_var",
                new_callable=AsyncMock,
                return_value="test-auth-code",
            ),
            patch(
                "prefect_server.pollSpice.DatetimeProvider.now",
                return_value=datetime(2025, 5, 1, tzinfo=UTC),
            ),
            patch(
                "prefect_server.pollSpice.Environment",
                return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock()),
            ),
            patch(
                "prefect_server.pollSpice.fetch_spice",
                return_value=[],
            ) as mock_fetch,
        ):
            await poll_spice_flow.fn(
                ingest_start_day=date(2025, 1, 1),
                ingest_end_date=date(2025, 1, 31),
            )

        mock_fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_automated_flow_uses_database_progress_dates(self):
        mock_date_manager = _make_mock_date_manager(
            start=date(2025, 2, 1),
            end=date(2025, 3, 1),
        )

        with (
            patch("prefect_server.pollSpice.get_run_logger", return_value=MagicMock()),
            patch(
                "prefect_server.pollSpice.Database",
                return_value=MagicMock(),
            ),
            patch(
                "prefect_server.pollSpice.DownloadDateManager",
                return_value=mock_date_manager,
            ),
            patch(
                "prefect_server.pollSpice.get_secret_or_env_var",
                new_callable=AsyncMock,
                return_value="test-auth-code",
            ),
            patch(
                "prefect_server.pollSpice.DatetimeProvider.now",
                return_value=datetime(2025, 5, 1, tzinfo=UTC),
            ),
            patch(
                "prefect_server.pollSpice.Environment",
                return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock()),
            ),
            patch(
                "prefect_server.pollSpice.fetch_spice",
                return_value=[],
            ) as mock_fetch,
            patch(
                "prefect_server.pollSpice.update_database_with_progress",
            ) as mock_update_db,
        ):
            await poll_spice_flow.fn()

        mock_fetch.assert_called_once()
        mock_update_db.assert_called_once()

    @pytest.mark.asyncio
    async def test_raises_when_no_download_dates_available(self):
        mock_date_manager = MagicMock()
        mock_date_manager.get_dates_for_download.return_value = None

        with (
            patch("prefect_server.pollSpice.get_run_logger", return_value=MagicMock()),
            patch(
                "prefect_server.pollSpice.Database",
                return_value=MagicMock(),
            ),
            patch(
                "prefect_server.pollSpice.DownloadDateManager",
                return_value=mock_date_manager,
            ),
            patch(
                "prefect_server.pollSpice.get_secret_or_env_var",
                new_callable=AsyncMock,
                return_value="test-auth-code",
            ),
            patch(
                "prefect_server.pollSpice.DatetimeProvider.now",
                return_value=datetime(2025, 5, 1, tzinfo=UTC),
            ),
        ):
            with pytest.raises(ValueError, match="No dates for download"):
                await poll_spice_flow.fn(
                    ingest_start_day=date(2025, 1, 1),
                )

    @pytest.mark.asyncio
    async def test_force_database_update_enables_db_update_on_manual_run(self):
        mock_date_manager = _make_mock_date_manager()
        mock_spice_file = (
            Path("spice/sclk/imap_sclk_0032.tsc"),
            MagicMock(),
            {"ingestion_date": "2025-01-15T12:00:00"},
        )

        with (
            patch("prefect_server.pollSpice.get_run_logger", return_value=MagicMock()),
            patch(
                "prefect_server.pollSpice.Database",
                return_value=MagicMock(),
            ),
            patch(
                "prefect_server.pollSpice.DownloadDateManager",
                return_value=mock_date_manager,
            ),
            patch(
                "prefect_server.pollSpice.get_secret_or_env_var",
                new_callable=AsyncMock,
                return_value="test-auth-code",
            ),
            patch(
                "prefect_server.pollSpice.DatetimeProvider.now",
                return_value=datetime(2025, 5, 1, tzinfo=UTC),
            ),
            patch(
                "prefect_server.pollSpice.Environment",
                return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock()),
            ),
            patch(
                "prefect_server.pollSpice.fetch_spice",
                return_value=[mock_spice_file],
            ),
            patch(
                "prefect_server.pollSpice.TimeConversion.try_extract_iso_like_datetime",
                return_value=datetime(2025, 1, 15, 12, 0, tzinfo=UTC),
            ),
            patch(
                "prefect_server.pollSpice.update_database_with_progress",
            ) as mock_update_db,
        ):
            await poll_spice_flow.fn(
                ingest_start_day=date(2025, 1, 1),
                ingest_end_date=date(2025, 1, 31),
                force_database_update=True,
            )

        mock_update_db.assert_called_once()

    @pytest.mark.asyncio
    async def test_does_not_update_database_when_not_automated_and_not_forced(self):
        mock_date_manager = _make_mock_date_manager()

        with (
            patch("prefect_server.pollSpice.get_run_logger", return_value=MagicMock()),
            patch(
                "prefect_server.pollSpice.Database",
                return_value=MagicMock(),
            ),
            patch(
                "prefect_server.pollSpice.DownloadDateManager",
                return_value=mock_date_manager,
            ),
            patch(
                "prefect_server.pollSpice.get_secret_or_env_var",
                new_callable=AsyncMock,
                return_value="test-auth-code",
            ),
            patch(
                "prefect_server.pollSpice.DatetimeProvider.now",
                return_value=datetime(2025, 5, 1, tzinfo=UTC),
            ),
            patch(
                "prefect_server.pollSpice.Environment",
                return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock()),
            ),
            patch(
                "prefect_server.pollSpice.fetch_spice",
                return_value=[],
            ),
            patch(
                "prefect_server.pollSpice.update_database_with_progress",
            ) as mock_update_db,
        ):
            await poll_spice_flow.fn(
                ingest_start_day=date(2025, 1, 1),
                ingest_end_date=date(2025, 1, 31),
                force_database_update=False,
            )

        mock_update_db.assert_not_called()

    @pytest.mark.asyncio
    async def test_adds_one_day_to_end_date_when_start_provided_but_not_end(self):
        mock_date_manager = _make_mock_date_manager()
        tomorrow_dt = datetime(2025, 5, 2, tzinfo=UTC)

        with (
            patch("prefect_server.pollSpice.get_run_logger", return_value=MagicMock()),
            patch("prefect_server.pollSpice.Database", return_value=MagicMock()),
            patch(
                "prefect_server.pollSpice.DownloadDateManager",
                return_value=mock_date_manager,
            ),
            patch(
                "prefect_server.pollSpice.get_secret_or_env_var",
                new_callable=AsyncMock,
                return_value="test-auth-code",
            ),
            patch(
                "prefect_server.pollSpice.DatetimeProvider.now",
                return_value=datetime(2025, 5, 1, tzinfo=UTC),
            ),
            patch(
                "prefect_server.pollSpice.DatetimeProvider.tomorrow",
                return_value=tomorrow_dt,
            ),
            patch(
                "prefect_server.pollSpice.Environment",
                return_value=MagicMock(__enter__=MagicMock(), __exit__=MagicMock()),
            ),
            patch(
                "prefect_server.pollSpice.fetch_spice",
                return_value=[],
            ),
        ):
            await poll_spice_flow.fn(
                ingest_start_day=date(2025, 1, 1),
                ingest_end_date=None,
            )

        call_args = mock_date_manager.get_dates_for_download.call_args
        passed_end = call_args[1].get("original_end_date") or call_args[0][1]
        assert passed_end == tomorrow_dt.date()
