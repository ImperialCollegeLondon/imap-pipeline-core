"""Unit tests for pollScience helper functions and flow name generation."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from imap_mag.util import ScienceLevel, ScienceMode
from prefect_server.pollScience import (
    _download_batch_of_science,
    _get_latest_ingestion_date,
    generate_flow_run_name,
    poll_science_flow,
)


class TestGetLatestIngestionDate:
    def _make_handler_with_ingestion(self, ingestion_date: datetime | None):
        handler = MagicMock()
        handler.ingestion_date = ingestion_date
        return handler

    def test_returns_max_ingestion_date_from_handlers(self):
        d1 = datetime(2025, 1, 1)
        d2 = datetime(2025, 6, 1)
        handlers = {
            Path("/a.cdf"): self._make_handler_with_ingestion(d1),
            Path("/b.cdf"): self._make_handler_with_ingestion(d2),
        }
        result = _get_latest_ingestion_date(handlers)
        assert result == d2

    def test_returns_none_when_all_ingestion_dates_are_none(self):
        handlers = {
            Path("/a.cdf"): self._make_handler_with_ingestion(None),
        }
        result = _get_latest_ingestion_date(handlers)
        assert result is None

    def test_ignores_none_ingestion_dates_and_returns_max(self):
        d1 = datetime(2025, 3, 15)
        handlers = {
            Path("/a.cdf"): self._make_handler_with_ingestion(None),
            Path("/b.cdf"): self._make_handler_with_ingestion(d1),
        }
        result = _get_latest_ingestion_date(handlers)
        assert result == d1

    def test_returns_none_for_empty_dict(self):
        result = _get_latest_ingestion_date({})
        assert result is None


class TestDownloadBatchOfScience:
    def _make_mock_logger(self):
        return MagicMock()

    def test_calls_fetch_science_with_correct_params(self):
        logger = self._make_mock_logger()
        mock_result = {}
        with (
            patch(
                "prefect_server.pollScience.fetch_science", return_value=mock_result
            ) as mock_fetch,
            patch("prefect_server.pollScience.update_database_with_progress"),
        ):
            result = _download_batch_of_science(
                level=MagicMock(),
                reference_frames=None,
                modes=None,
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 31),
                logger=logger,
                database=MagicMock(),
                use_database=False,
                use_ingestion_date=False,
                progress_item_id="TEST_L1C",
                packet_start_timestamp=datetime(2025, 1, 1),
                batch_size=10,
                skip_items_count=0,
            )
        mock_fetch.assert_called_once()
        assert result == mock_result

    def test_updates_database_when_use_database_is_true(self):
        logger = self._make_mock_logger()
        mock_handler = MagicMock()
        mock_handler.ingestion_date = datetime(2025, 5, 1)
        mock_result = {Path("/a.cdf"): mock_handler}

        with (
            patch("prefect_server.pollScience.fetch_science", return_value=mock_result),
            patch(
                "prefect_server.pollScience.update_database_with_progress"
            ) as mock_update,
        ):
            _download_batch_of_science(
                level=MagicMock(),
                reference_frames=None,
                modes=None,
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 31),
                logger=logger,
                database=MagicMock(),
                use_database=True,
                use_ingestion_date=True,
                progress_item_id="TEST_L1C",
                packet_start_timestamp=datetime(2025, 1, 1),
                batch_size=10,
                skip_items_count=0,
            )
        mock_update.assert_called_once()

    def test_does_not_update_database_when_use_database_is_false(self):
        logger = self._make_mock_logger()
        mock_result = {}

        with (
            patch("prefect_server.pollScience.fetch_science", return_value=mock_result),
            patch(
                "prefect_server.pollScience.update_database_with_progress"
            ) as mock_update,
        ):
            _download_batch_of_science(
                level=MagicMock(),
                reference_frames=None,
                modes=None,
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 31),
                logger=logger,
                database=MagicMock(),
                use_database=False,
                use_ingestion_date=False,
                progress_item_id="TEST_L1C",
                packet_start_timestamp=datetime(2025, 1, 1),
                batch_size=10,
                skip_items_count=0,
            )
        mock_update.assert_not_called()


class TestPollScienceFlowUnit:
    """Unit tests for poll_science_flow without Docker."""

    @pytest.mark.asyncio
    async def test_logs_warning_when_force_database_update_without_force_ingestion_date(
        self,
    ):
        mock_logger = MagicMock()
        mock_dm = MagicMock()
        mock_dm.get_dates_for_download.return_value = None

        with (
            patch(
                "prefect_server.pollScience.try_get_prefect_logger",
                return_value=mock_logger,
            ),
            patch("prefect_server.pollScience.Database", return_value=MagicMock()),
            patch(
                "prefect_server.pollScience.get_secret_or_env_var", return_value="code"
            ),
            patch(
                "prefect_server.pollScience.DownloadDateManager", return_value=mock_dm
            ),
        ):
            await poll_science_flow.fn(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 31),
                force_database_update=True,
                force_ingestion_date=False,
            )

        mock_logger.warning.assert_any_call(
            "Database cannot be updated without forcing ingestion date. Database will not be updated."
        )

    @pytest.mark.asyncio
    async def test_uses_single_mode_in_progress_item_id(self):
        mock_logger = MagicMock()
        mock_dm = MagicMock()
        mock_dm.get_dates_for_download.return_value = None

        with (
            patch(
                "prefect_server.pollScience.try_get_prefect_logger",
                return_value=mock_logger,
            ),
            patch("prefect_server.pollScience.Database", return_value=MagicMock()),
            patch(
                "prefect_server.pollScience.get_secret_or_env_var", return_value="code"
            ),
            patch(
                "prefect_server.pollScience.DownloadDateManager", return_value=mock_dm
            ),
        ):
            await poll_science_flow.fn(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 31),
                modes=[ScienceMode.Normal],
            )

        mock_dm.get_dates_for_download.assert_called_once()

    @pytest.mark.asyncio
    async def test_downloads_items_and_extends_list_when_batch_returns_results(self):
        mock_logger = MagicMock()
        mock_dm = MagicMock()
        mock_dm.get_dates_for_download.return_value = (
            datetime(2025, 1, 1),
            datetime(2025, 1, 31),
        )

        fake_path = Path("/some/science.cdf")
        mock_items = {fake_path: MagicMock()}

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return mock_items
            return {}

        with (
            patch(
                "prefect_server.pollScience.try_get_prefect_logger",
                return_value=mock_logger,
            ),
            patch("prefect_server.pollScience.Database", return_value=MagicMock()),
            patch(
                "prefect_server.pollScience.get_secret_or_env_var", return_value="code"
            ),
            patch(
                "prefect_server.pollScience.DownloadDateManager", return_value=mock_dm
            ),
            patch(
                "prefect_server.pollScience._download_batch_of_science",
                side_effect=side_effect,
            ),
            patch("asyncio.sleep"),
        ):
            await poll_science_flow.fn(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 31),
            )

        mock_logger.info.assert_called()

    @pytest.mark.asyncio
    async def test_loop_continues_to_second_batch_when_full_batch_returned(self):
        mock_logger = MagicMock()
        mock_dm = MagicMock()
        mock_dm.get_dates_for_download.return_value = (
            datetime(2025, 1, 1),
            datetime(2025, 1, 31),
        )

        fake_path_1 = Path("/science1.cdf")
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {fake_path_1: MagicMock()}
            return {}

        with (
            patch(
                "prefect_server.pollScience.try_get_prefect_logger",
                return_value=mock_logger,
            ),
            patch("prefect_server.pollScience.Database", return_value=MagicMock()),
            patch(
                "prefect_server.pollScience.get_secret_or_env_var", return_value="code"
            ),
            patch(
                "prefect_server.pollScience.DownloadDateManager", return_value=mock_dm
            ),
            patch(
                "prefect_server.pollScience._download_batch_of_science",
                side_effect=side_effect,
            ),
            patch("prefect_server.pollScience.BATCH_SIZE", 1),
            patch("asyncio.sleep"),
        ):
            await poll_science_flow.fn(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 31),
            )

        assert call_count == 2

    @pytest.mark.asyncio
    async def test_warns_and_stops_when_same_first_item_in_consecutive_batches(self):
        mock_logger = MagicMock()
        mock_dm = MagicMock()
        mock_dm.get_dates_for_download.return_value = (
            datetime(2025, 1, 1),
            datetime(2025, 1, 31),
        )

        same_path = Path("/science_duplicate.cdf")

        def side_effect(*args, **kwargs):
            return {same_path: MagicMock()}

        with (
            patch(
                "prefect_server.pollScience.try_get_prefect_logger",
                return_value=mock_logger,
            ),
            patch("prefect_server.pollScience.Database", return_value=MagicMock()),
            patch(
                "prefect_server.pollScience.get_secret_or_env_var", return_value="code"
            ),
            patch(
                "prefect_server.pollScience.DownloadDateManager", return_value=mock_dm
            ),
            patch(
                "prefect_server.pollScience._download_batch_of_science",
                side_effect=side_effect,
            ),
            patch("prefect_server.pollScience.BATCH_SIZE", 1),
            patch("asyncio.sleep"),
        ):
            await poll_science_flow.fn(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 31),
            )

        mock_logger.warning.assert_any_call(
            "First item in batch is the same as the previous batch. Stopping to avoid infinite loop."
        )


class TestPollScienceFlowGenerateName:
    def test_auto_run_includes_last_update(self):
        mock_params = {
            "level": ScienceLevel.l2,
            "modes": [ScienceMode.Normal],
            "start_date": None,
            "end_date": None,
        }
        with patch("prefect_server.pollScience.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name()

        assert "last-update" in name

    def test_specific_dates_in_name(self):
        mock_params = {
            "level": ScienceLevel.l1c,
            "modes": [ScienceMode.Normal],
            "start_date": datetime(2025, 6, 1),
            "end_date": datetime(2025, 6, 30),
        }
        with patch("prefect_server.pollScience.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name()

        assert "01-06-2025" in name
