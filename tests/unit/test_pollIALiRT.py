"""Unit tests for pollIALiRT helpers, flow logic, and name generation."""

from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from imap_mag.util.DatetimeProvider import DatetimeProvider
from prefect_server.pollIALiRT import (
    PollIALiRTFlow,
    _do_poll,
    do_poll_ialirt,
)


class TestDoPollUnit:
    """Unit tests for _do_poll helper function without Docker."""

    def _make_mock_db(self, packet_dates=None):
        mock_db = MagicMock()
        mock_date_manager = MagicMock()
        mock_date_manager.get_dates_for_download.return_value = packet_dates
        return mock_db, mock_date_manager

    def test_returns_empty_list_when_no_dates_to_download(self):
        logger = MagicMock()
        mock_db = MagicMock()
        fetch_fn = MagicMock(return_value={})

        with patch("prefect_server.pollIALiRT.DownloadDateManager") as mock_dm_class:
            mock_dm = MagicMock()
            mock_dm.get_dates_for_download.return_value = None
            mock_dm_class.return_value = mock_dm

            result = _do_poll(
                database=mock_db,
                auth_code="test-key",
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                force_download=False,
                logger=logger,
                progress_item_id="TEST_ITEM",
                fetch_fn=fetch_fn,
                event_type=None,
            )

        assert result == []
        fetch_fn.assert_not_called()

    def test_returns_empty_list_when_fetch_returns_no_data(self):
        logger = MagicMock()
        mock_db = MagicMock()
        dp = DatetimeProvider(fixed_now=datetime(2025, 1, 1))

        with (
            patch("prefect_server.pollIALiRT.DownloadDateManager") as mock_dm_class,
            patch("prefect_server.pollIALiRT.update_database_with_progress"),
        ):
            mock_dm = MagicMock()
            mock_dm.get_dates_for_download.return_value = (
                datetime(2025, 1, 1),
                datetime(2025, 1, 2),
            )
            mock_dm_class.return_value = mock_dm

            fetch_fn = MagicMock(return_value={})

            result = _do_poll(
                database=mock_db,
                auth_code="test-key",
                start_date=None,
                end_date=None,
                force_download=False,
                logger=logger,
                progress_item_id="TEST_ITEM",
                fetch_fn=fetch_fn,
                event_type=None,
                datetime_provider=dp,
            )

        assert result == []
        logger.info.assert_called()

    def test_updates_database_progress_when_data_is_downloaded(self):
        logger = MagicMock()
        mock_db = MagicMock()
        mock_handler = MagicMock()
        mock_handler.content_date = datetime(2025, 1, 1, 12, 0, 0)
        downloaded_files = {Path("/some/file.csv"): mock_handler}
        dp = DatetimeProvider(fixed_now=datetime(2025, 1, 1))

        with (
            patch("prefect_server.pollIALiRT.DownloadDateManager") as mock_dm_class,
            patch(
                "prefect_server.pollIALiRT.update_database_with_progress"
            ) as mock_update,
        ):
            mock_dm = MagicMock()
            mock_dm.get_dates_for_download.return_value = (
                datetime(2025, 1, 1),
                datetime(2025, 1, 2),
            )
            mock_dm_class.return_value = mock_dm

            fetch_fn = MagicMock(return_value=downloaded_files)

            result = _do_poll(
                database=mock_db,
                auth_code="test-key",
                start_date=None,
                end_date=None,
                force_download=False,
                logger=logger,
                progress_item_id="TEST_ITEM",
                fetch_fn=fetch_fn,
                event_type=None,
                datetime_provider=dp,
            )

        assert result == list(downloaded_files.keys())
        mock_update.assert_called_once()

    def test_emits_event_when_event_type_provided(self):
        logger = MagicMock()
        mock_db = MagicMock()
        mock_handler = MagicMock()
        mock_handler.content_date = datetime(2025, 1, 1, 12, 0, 0)
        downloaded_files = {Path("/some/file.csv"): mock_handler}
        dp = DatetimeProvider(fixed_now=datetime(2025, 1, 1))

        with (
            patch("prefect_server.pollIALiRT.DownloadDateManager") as mock_dm_class,
            patch("prefect_server.pollIALiRT.update_database_with_progress"),
            patch("prefect_server.pollIALiRT.emit_event") as mock_emit,
            patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run,
        ):
            mock_flow_run.id = "test-flow-run-id"
            mock_flow_run.name = "test-flow-run"
            mock_dm = MagicMock()
            mock_dm.get_dates_for_download.return_value = (
                datetime(2025, 1, 1),
                datetime(2025, 1, 2),
            )
            mock_dm_class.return_value = mock_dm

            fetch_fn = MagicMock(return_value=downloaded_files)
            mock_emit.return_value = MagicMock()

            _do_poll(
                database=mock_db,
                auth_code="test-key",
                start_date=None,
                end_date=None,
                force_download=False,
                logger=logger,
                progress_item_id="TEST_ITEM",
                fetch_fn=fetch_fn,
                event_type="ialirt-hk-updated",
                datetime_provider=dp,
            )

        mock_emit.assert_called_once()

    def test_logs_error_when_event_emission_fails(self):
        logger = MagicMock()
        mock_db = MagicMock()
        mock_handler = MagicMock()
        mock_handler.content_date = datetime(2025, 1, 1, 12, 0, 0)
        downloaded_files = {Path("/some/file.csv"): mock_handler}
        dp = DatetimeProvider(fixed_now=datetime(2025, 1, 1))

        with (
            patch("prefect_server.pollIALiRT.DownloadDateManager") as mock_dm_class,
            patch("prefect_server.pollIALiRT.update_database_with_progress"),
            patch("prefect_server.pollIALiRT.emit_event", return_value=None),
            patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run,
        ):
            mock_flow_run.id = "test-flow-run-id"
            mock_flow_run.name = "test-flow-run"
            mock_dm = MagicMock()
            mock_dm.get_dates_for_download.return_value = (
                datetime(2025, 1, 1),
                datetime(2025, 1, 2),
            )
            mock_dm_class.return_value = mock_dm

            fetch_fn = MagicMock(return_value=downloaded_files)

            _do_poll(
                database=mock_db,
                auth_code="test-key",
                start_date=None,
                end_date=None,
                force_download=False,
                logger=logger,
                progress_item_id="TEST_ITEM",
                fetch_fn=fetch_fn,
                event_type="ialirt-hk-updated",
                datetime_provider=dp,
            )

        logger.error.assert_called_once()


class TestPollIALiRTFlowUnit:
    """Unit tests for poll_ialirt_flow without Docker."""

    @pytest.mark.asyncio
    async def test_flow_calls_do_poll_ialirt_and_returns(self):
        mock_logger = MagicMock()
        mock_db = MagicMock()
        dp = DatetimeProvider(fixed_now=datetime(2025, 1, 1, 23, 59))
        flow_instance = PollIALiRTFlow(datetime_provider=dp)

        with (
            patch(
                "prefect_server.pollIALiRT.try_get_prefect_logger",
                return_value=mock_logger,
            ),
            patch("prefect_server.pollIALiRT.Database", return_value=mock_db),
            patch(
                "prefect_server.pollIALiRT.get_secret_or_env_var",
                return_value="auth-code",
            ),
            patch(
                "prefect_server.pollIALiRT.do_poll_ialirt", return_value=[]
            ) as mock_do_poll,
        ):
            await flow_instance.poll_ialirt_flow.fn(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                wait_for_new_data_to_arrive=False,
                plot_last_3_days=False,
            )

        mock_do_poll.assert_called_once()
        mock_logger.info.assert_called()

    @pytest.mark.asyncio
    async def test_flow_generates_quicklook_when_plot_last_3_days_true(self):
        mock_logger = MagicMock()
        mock_db = MagicMock()
        dp = DatetimeProvider(fixed_now=datetime(2025, 1, 1, 23, 59))
        flow_instance = PollIALiRTFlow(datetime_provider=dp)

        with (
            patch(
                "prefect_server.pollIALiRT.try_get_prefect_logger",
                return_value=mock_logger,
            ),
            patch("prefect_server.pollIALiRT.Database", return_value=mock_db),
            patch(
                "prefect_server.pollIALiRT.get_secret_or_env_var",
                return_value="auth-code",
            ),
            patch("prefect_server.pollIALiRT.do_poll_ialirt", return_value=[]),
            patch(
                "prefect_server.pollIALiRT.quicklook_ialirt_flow",
                new_callable=AsyncMock,
            ) as mock_quicklook,
        ):
            mock_quicklook.return_value = None
            await flow_instance.poll_ialirt_flow.fn(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                wait_for_new_data_to_arrive=False,
                plot_last_3_days=True,
            )

        mock_quicklook.assert_called_once()


class TestPollIALiRTHKFlowUnit:
    """Unit tests for poll_ialirt_hk_flow without Docker."""

    @pytest.mark.asyncio
    async def test_flow_calls_do_poll_ialirt_hk_and_returns(self):
        mock_logger = MagicMock()
        mock_db = MagicMock()
        dp = DatetimeProvider(fixed_now=datetime(2025, 1, 1, 23, 59))
        flow_instance = PollIALiRTFlow(datetime_provider=dp)

        with (
            patch(
                "prefect_server.pollIALiRT.try_get_prefect_logger",
                return_value=mock_logger,
            ),
            patch("prefect_server.pollIALiRT.Database", return_value=mock_db),
            patch(
                "prefect_server.pollIALiRT.get_secret_or_env_var",
                return_value="auth-code",
            ),
            patch(
                "prefect_server.pollIALiRT.do_poll_ialirt_hk", return_value=[]
            ) as mock_do_poll,
        ):
            await flow_instance.poll_ialirt_hk_flow.fn(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                wait_for_new_data_to_arrive=False,
            )

        mock_do_poll.assert_called_once()
        mock_logger.info.assert_called()

    @pytest.mark.asyncio
    async def test_flow_polls_once_when_wait_for_new_data_is_true_and_exits(self):
        mock_logger = MagicMock()
        mock_db = MagicMock()
        # First call: 3600s remaining > timeout (5*60=300), enters loop
        # Second call: 299s remaining, while exits
        now_calls = [datetime(2025, 1, 1, 0, 0, 0), datetime(2025, 1, 1, 0, 55, 1)]

        def fake_now():
            return now_calls.pop(0) if now_calls else datetime(2025, 1, 1, 1, 0, 0)

        end_date = datetime(2025, 1, 1, 1, 0, 0)
        dp = DatetimeProvider()
        flow_instance = PollIALiRTFlow(datetime_provider=dp)

        with (
            patch(
                "prefect_server.pollIALiRT.try_get_prefect_logger",
                return_value=mock_logger,
            ),
            patch("prefect_server.pollIALiRT.Database", return_value=mock_db),
            patch(
                "prefect_server.pollIALiRT.get_secret_or_env_var",
                return_value="auth-code",
            ),
            patch.object(dp, "now", side_effect=fake_now),
            patch(
                "prefect_server.pollIALiRT.do_poll_ialirt", return_value=[]
            ) as mock_do_poll,
            patch("prefect_server.pollIALiRT.asyncio.sleep"),
        ):
            await flow_instance.poll_ialirt_flow.fn(
                end_date=end_date,
                wait_for_new_data_to_arrive=True,
                plot_last_3_days=False,
            )

        mock_do_poll.assert_called_once()


class TestPollIALiRTHKFlowUnitExtended:
    """Additional unit tests for poll_ialirt_hk_flow."""

    @pytest.mark.asyncio
    async def test_hk_flow_polls_once_when_wait_for_new_data_is_true(self):
        mock_logger = MagicMock()
        mock_db = MagicMock()
        now_calls = [datetime(2025, 1, 1, 0, 0, 0), datetime(2025, 1, 1, 0, 55, 1)]

        def fake_now():
            return now_calls.pop(0) if now_calls else datetime(2025, 1, 1, 1, 0, 0)

        end_date = datetime(2025, 1, 1, 1, 0, 0)
        dp = DatetimeProvider()
        flow_instance = PollIALiRTFlow(datetime_provider=dp)

        with (
            patch(
                "prefect_server.pollIALiRT.try_get_prefect_logger",
                return_value=mock_logger,
            ),
            patch("prefect_server.pollIALiRT.Database", return_value=mock_db),
            patch(
                "prefect_server.pollIALiRT.get_secret_or_env_var",
                return_value="auth-code",
            ),
            patch.object(dp, "now", side_effect=fake_now),
            patch(
                "prefect_server.pollIALiRT.do_poll_ialirt_hk", return_value=[]
            ) as mock_do_poll,
            patch("prefect_server.pollIALiRT.asyncio.sleep"),
        ):
            await flow_instance.poll_ialirt_hk_flow.fn(
                end_date=end_date,
                wait_for_new_data_to_arrive=True,
            )

        mock_do_poll.assert_called_once()


class TestPollIALiRTGenerateName:
    def test_name_with_no_dates_uses_last_update(self):
        mock_params = {"start_date": None, "end_date": None}
        flow_instance = PollIALiRTFlow(
            datetime_provider=DatetimeProvider(fixed_now=datetime(2025, 6, 1))
        )
        with patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = flow_instance._generate_flow_run_name()

        assert "last-update" in name

    def test_name_with_dates(self):
        mock_params = {
            "start_date": datetime(2025, 6, 1, 12, 0, 0),
            "end_date": datetime(2025, 6, 1, 13, 0, 0),
        }
        flow_instance = PollIALiRTFlow()
        with patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = flow_instance._generate_flow_run_name()

        assert "01-06-2025" in name

    def test_hk_name_with_no_dates(self):
        mock_params = {"start_date": None, "end_date": None}
        flow_instance = PollIALiRTFlow(
            datetime_provider=DatetimeProvider(fixed_now=datetime(2025, 6, 1))
        )
        with patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = flow_instance._generate_hk_flow_run_name()

        assert "HK" in name


class TestDoPollIALiRT:
    def test_do_poll_ialirt_returns_downloaded_files(self, tmp_path):
        downloaded_file = tmp_path / "ialirt.csv"
        downloaded_file.touch()
        mock_handler = MagicMock()
        mock_handler.content_date = datetime(2025, 1, 15)

        with (
            patch("prefect_server.pollIALiRT.DownloadDateManager") as mock_dm,
            patch(
                "prefect_server.pollIALiRT.fetch_ialirt",
                return_value={downloaded_file: mock_handler},
            ),
            patch("prefect_server.pollIALiRT.update_database_with_progress"),
        ):
            mock_dm_instance = MagicMock()
            mock_dm_instance.get_dates_for_download.return_value = (
                datetime(2025, 1, 1),
                datetime(2025, 1, 31),
            )
            mock_dm.return_value = mock_dm_instance

            result = do_poll_ialirt(
                database=MagicMock(),
                auth_code="auth",
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 31),
                force_download=False,
                logger=MagicMock(),
                datetime_provider=DatetimeProvider(fixed_now=datetime(2025, 1, 1)),
            )

        assert downloaded_file in result
