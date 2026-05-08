import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytz

from imap_mag.config.AppSettings import AppSettings
from imap_mag.util import DatetimeProvider, Environment
from prefect_server.pollIALiRT import (
    _do_poll,
    do_poll_ialirt,
    generate_flow_run_name,
    generate_hk_flow_run_name,
    poll_ialirt_flow,
    poll_ialirt_hk_flow,
)
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    END_OF_HOUR,
    NOW,
    TODAY,
    YESTERDAY,
    mock_datetime_provider,  # noqa: F401
)
from tests.util.prefect_test_utils import (  # noqa: F401
    mock_teams_webhook_block,
    prefect_test_fixture,
)


def define_available_ialirt_mappings(
    wiremock_manager,
    start_date: datetime,
    end_date: datetime,
):
    start_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%S")

    query_response: list[dict] = [
        {
            "mag_B_GSE": [-1.53, -3.033, 0.539],
            "mag_theta_B_GSM": 25.017,
            "time_utc": start_date_str,
            "extra_field1": "extra_value1",
        },
        {
            "mag_B_GSE": [4.187, 0.687, 0.757],
            "mag_theta_B_GSM": 6.732,
            "time_utc": end_date_str,
            "extra_field2": "extra_value2",
        },
    ]

    # Use pattern matching so chunked requests (any date range) for mag instrument are handled.
    # Including end_date_str in the response causes the chunking loop to terminate after one chunk.
    wiremock_manager.add_string_mapping(
        r"/space-weather\?instrument=mag&.*",
        json.dumps({"meta": {"count": 2, "instrument": "mag"}, "data": query_response}),
        is_pattern=True,
        priority=1,
    )


def define_available_ialirt_hk_mappings(
    wiremock_manager,
    start_date: datetime,
    end_date: datetime,
):
    start_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%S")

    query_response: list[dict] = [
        {
            "instrument": "mag_hk",
            "mag_hk_status": {
                "fib_temp": 2464,
                "mode": 6,
                "hk1v5c_warn": False,
                "hk3v3": 2880,
                "fob_range": 2,
            },
            "time_utc": start_date_str,
        },
        {
            "instrument": "mag_hk",
            "mag_hk_status": {
                "fib_temp": 2464,
                "mode": 5,
                "hk1v5c_warn": False,
                "hk3v3": 2880,
                "fob_range": 2,
            },
            "time_utc": end_date_str,
        },
    ]

    # Use pattern matching so chunked requests (any date range) for mag_hk instrument are handled.
    # Including end_date_str in the response causes the chunking loop to terminate after one chunk.
    wiremock_manager.add_string_mapping(
        r"/space-weather\?instrument=mag_hk&.*",
        json.dumps(
            {"meta": {"count": 2, "instrument": "mag_hk"}, "data": query_response}
        ),
        is_pattern=True,
        priority=1,
    )


def verify_available_ialirt(
    database,
    progress_timestamp: datetime,
    actual_timestamp: datetime,
):
    # Database.
    workflow_progress = database.get_workflow_progress("MAG_IALIRT")

    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() == progress_timestamp

    # Files.
    check_file_existence(actual_timestamp, "ialirt", "imap_ialirt")


def verify_available_ialirt_hk(
    database,
    progress_timestamp: datetime,
    actual_timestamp: datetime,
):
    # Database.
    workflow_progress = database.get_workflow_progress("MAG_IALIRT_HK")

    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() == progress_timestamp

    # Files.
    check_file_existence(actual_timestamp, "ialirt_hk", "imap_ialirt_hk")


def check_file_existence(
    actual_timestamp: datetime, folder_name: str, file_prefix: str
):
    datastore = AppSettings().data_store  # type: ignore
    data_folder = os.path.join(
        datastore, folder_name, actual_timestamp.strftime("%Y/%m")
    )
    cdf_file = f"{file_prefix}_{actual_timestamp.strftime('%Y%m%d')}.csv"

    assert os.path.exists(os.path.join(data_folder, cdf_file))


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_ialirt_autoflow_first_ever_run(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    clean_datastore,
):
    # Set up.
    wiremock_manager.reset()

    define_available_ialirt_mappings(wiremock_manager, YESTERDAY, END_OF_HOUR)

    # Exercise.
    with Environment(
        IALIRT_DATA_ACCESS_URL=wiremock_manager.get_url().rstrip("/"),
        IALIRT_API_KEY="12345",
    ):
        await poll_ialirt_flow(
            wait_for_new_data_to_arrive=False, plot_last_3_days=False
        )

    # Verify.
    verify_available_ialirt(
        test_database,
        END_OF_HOUR.replace(microsecond=0),  # I-ALiRT does not use microsecond accuracy
        TODAY,
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_ialirt_autoflow_continue_from_previous_download(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    clean_datastore,
):
    # Set up.
    progress_timestamp = TODAY + timedelta(hours=5, minutes=30)

    workflow_progress = test_database.get_workflow_progress("MAG_IALIRT")
    workflow_progress.update_progress_timestamp(progress_timestamp)

    test_database.save(workflow_progress)
    wiremock_manager.reset()

    define_available_ialirt_mappings(
        wiremock_manager, progress_timestamp + timedelta(seconds=1), END_OF_HOUR
    )

    # Exercise.
    with Environment(
        IALIRT_DATA_ACCESS_URL=wiremock_manager.get_url().rstrip("/"),
        IALIRT_API_KEY="12345",
    ):
        await poll_ialirt_flow(
            wait_for_new_data_to_arrive=False, plot_last_3_days=False
        )

    # Verify.
    verify_available_ialirt(
        test_database,
        END_OF_HOUR.replace(microsecond=0),  # I-ALiRT does not use microsecond accuracy
        TODAY,
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_ialirt_autoflow_specify_start_end_dates(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    clean_datastore,
):
    # Set up.
    start_date = datetime(2025, 4, 1)
    end_date = datetime(2025, 4, 2)

    wiremock_manager.reset()

    define_available_ialirt_mappings(wiremock_manager, start_date, end_date)

    # Exercise.
    with Environment(
        IALIRT_DATA_ACCESS_URL=wiremock_manager.get_url().rstrip("/"),
        IALIRT_API_KEY="12345",
    ):
        await poll_ialirt_flow(
            wait_for_new_data_to_arrive=False,
            plot_last_3_days=False,
            start_date=start_date,
            end_date=end_date,
        )

    # Verify.
    verify_available_ialirt(
        test_database,
        end_date,
        start_date,
    )


# Force the next test to be at 6 AM UK time, and only do 1 iteration of polling
NOW_ALMOST_END_OF_HOUR_6AM_UK_TIME = (
    pytz.timezone("Europe/London")
    .localize(
        NOW.replace(
            hour=6,
            minute=59,
            second=54,
            microsecond=0,
        )
    )
    .astimezone(UTC)
    .replace(tzinfo=None)
)


@pytest.fixture(autouse=False)
def mock_datetime_provider_for_6am_uk_time(monkeypatch):
    now = NOW_ALMOST_END_OF_HOUR_6AM_UK_TIME
    today = NOW_ALMOST_END_OF_HOUR_6AM_UK_TIME.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    yesterday = today - timedelta(days=1)
    end_of_hour = now.replace(minute=59, second=59, microsecond=999999)
    num_calls = -1

    def return_now():
        nonlocal num_calls
        num_calls += 1
        return now + timedelta(seconds=5 * num_calls)

    monkeypatch.setattr(DatetimeProvider, "now", return_now)
    monkeypatch.setattr(
        DatetimeProvider,
        "today",
        lambda: today,
    )
    monkeypatch.setattr(DatetimeProvider, "yesterday", lambda: yesterday)
    monkeypatch.setattr(
        DatetimeProvider,
        "end_of_hour",
        lambda: end_of_hour,
    )

    return (yesterday, end_of_hour)


@pytest.fixture
def mock_quicklook_ialirt_flow(mocker) -> None:
    mocker.patch(
        "prefect_server.pollIALiRT.quicklook_ialirt_flow",
        new=mock.AsyncMock(return_value=None),
    )


@pytest.mark.skipif(sys.version_info < (3, 13), reason="Requires python3.13 or higher")
@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_ialirt_send_quicklook_at_6am_uk_time(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider_for_6am_uk_time,
    mock_quicklook_ialirt_flow,
    prefect_test_fixture,  # noqa: F811
    mock_teams_webhook_block,  # noqa: F811
    clean_datastore,
):
    # Set up.
    wiremock_manager.reset()

    yesterday, end_of_hour = mock_datetime_provider_for_6am_uk_time
    define_available_ialirt_mappings(wiremock_manager, yesterday, end_of_hour)

    # Exercise.
    with Environment(
        IALIRT_DATA_ACCESS_URL=wiremock_manager.get_url().rstrip("/"),
        IALIRT_API_KEY="12345",
    ):
        await poll_ialirt_flow(
            wait_for_new_data_to_arrive=True,
            timeout=5,
            plot_last_3_days=True,
        )

    # Verify.
    mock_teams_webhook_block.notify.assert_called_once()


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_ialirt_hk_autoflow_first_ever_run(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    clean_datastore,
):
    # Set up.
    wiremock_manager.reset()

    define_available_ialirt_hk_mappings(wiremock_manager, YESTERDAY, END_OF_HOUR)

    # Exercise.
    with Environment(
        IALIRT_DATA_ACCESS_URL=wiremock_manager.get_url().rstrip("/"),
        IALIRT_API_KEY="12345",
    ):
        await poll_ialirt_hk_flow(
            wait_for_new_data_to_arrive=False,
        )

    # Verify.
    verify_available_ialirt_hk(
        test_database,
        END_OF_HOUR.replace(microsecond=0),
        TODAY,
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_ialirt_hk_autoflow_specify_start_end_dates(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    clean_datastore,
):
    # Set up.
    start_date = datetime(2025, 4, 1)
    end_date = datetime(2025, 4, 2)

    wiremock_manager.reset()

    define_available_ialirt_hk_mappings(wiremock_manager, start_date, end_date)

    # Exercise.
    with Environment(
        IALIRT_DATA_ACCESS_URL=wiremock_manager.get_url().rstrip("/"),
        IALIRT_API_KEY="12345",
    ):
        await poll_ialirt_hk_flow(
            wait_for_new_data_to_arrive=False,
            start_date=start_date,
            end_date=end_date,
        )

    # Verify.
    verify_available_ialirt_hk(
        test_database,
        end_date,
        start_date,
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

        with (
            patch("prefect_server.pollIALiRT.DownloadDateManager") as mock_dm_class,
            patch("prefect_server.pollIALiRT.update_database_with_progress"),
            patch(
                "prefect_server.pollIALiRT.DatetimeProvider.now",
                return_value=datetime(2025, 1, 1),
            ),
            patch(
                "prefect_server.pollIALiRT.DatetimeProvider.yesterday",
                return_value=datetime(2024, 12, 31),
            ),
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
            )

        assert result == []
        logger.info.assert_called()

    def test_updates_database_progress_when_data_is_downloaded(self):
        logger = MagicMock()
        mock_db = MagicMock()
        mock_handler = MagicMock()
        mock_handler.content_date = datetime(2025, 1, 1, 12, 0, 0)
        downloaded_files = {Path("/some/file.csv"): mock_handler}

        with (
            patch("prefect_server.pollIALiRT.DownloadDateManager") as mock_dm_class,
            patch(
                "prefect_server.pollIALiRT.update_database_with_progress"
            ) as mock_update,
            patch(
                "prefect_server.pollIALiRT.DatetimeProvider.now",
                return_value=datetime(2025, 1, 1),
            ),
            patch(
                "prefect_server.pollIALiRT.DatetimeProvider.yesterday",
                return_value=datetime(2024, 12, 31),
            ),
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
            )

        assert result == list(downloaded_files.keys())
        mock_update.assert_called_once()

    def test_emits_event_when_event_type_provided(self):
        logger = MagicMock()
        mock_db = MagicMock()
        mock_handler = MagicMock()
        mock_handler.content_date = datetime(2025, 1, 1, 12, 0, 0)
        downloaded_files = {Path("/some/file.csv"): mock_handler}

        with (
            patch("prefect_server.pollIALiRT.DownloadDateManager") as mock_dm_class,
            patch("prefect_server.pollIALiRT.update_database_with_progress"),
            patch("prefect_server.pollIALiRT.emit_event") as mock_emit,
            patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run,
            patch(
                "prefect_server.pollIALiRT.DatetimeProvider.now",
                return_value=datetime(2025, 1, 1),
            ),
            patch(
                "prefect_server.pollIALiRT.DatetimeProvider.yesterday",
                return_value=datetime(2024, 12, 31),
            ),
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
            )

        mock_emit.assert_called_once()

    def test_logs_error_when_event_emission_fails(self):
        logger = MagicMock()
        mock_db = MagicMock()
        mock_handler = MagicMock()
        mock_handler.content_date = datetime(2025, 1, 1, 12, 0, 0)
        downloaded_files = {Path("/some/file.csv"): mock_handler}

        with (
            patch("prefect_server.pollIALiRT.DownloadDateManager") as mock_dm_class,
            patch("prefect_server.pollIALiRT.update_database_with_progress"),
            patch("prefect_server.pollIALiRT.emit_event", return_value=None),
            patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run,
            patch(
                "prefect_server.pollIALiRT.DatetimeProvider.now",
                return_value=datetime(2025, 1, 1),
            ),
            patch(
                "prefect_server.pollIALiRT.DatetimeProvider.yesterday",
                return_value=datetime(2024, 12, 31),
            ),
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
            )

        logger.error.assert_called_once()


class TestPollIALiRTFlowUnit:
    """Unit tests for poll_ialirt_flow without Docker."""

    @pytest.mark.asyncio
    async def test_flow_calls_do_poll_ialirt_and_returns(self):
        mock_logger = MagicMock()
        mock_db = MagicMock()

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
                "prefect_server.pollIALiRT.DatetimeProvider.end_of_hour",
                return_value=datetime(2025, 1, 1, 23, 59),
            ),
            patch(
                "prefect_server.pollIALiRT.do_poll_ialirt", return_value=[]
            ) as mock_do_poll,
        ):
            await poll_ialirt_flow.fn(
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
                "prefect_server.pollIALiRT.DatetimeProvider.end_of_hour",
                return_value=datetime(2025, 1, 1, 23, 59),
            ),
            patch("prefect_server.pollIALiRT.do_poll_ialirt", return_value=[]),
            patch(
                "prefect_server.pollIALiRT.quicklook_ialirt_flow",
                new_callable=AsyncMock,
            ) as mock_quicklook,
        ):
            mock_quicklook.return_value = None
            await poll_ialirt_flow.fn(
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
                "prefect_server.pollIALiRT.DatetimeProvider.end_of_hour",
                return_value=datetime(2025, 1, 1, 23, 59),
            ),
            patch(
                "prefect_server.pollIALiRT.do_poll_ialirt_hk", return_value=[]
            ) as mock_do_poll,
        ):
            await poll_ialirt_hk_flow.fn(
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
                "prefect_server.pollIALiRT.DatetimeProvider.end_of_hour",
                return_value=end_date,
            ),
            patch(
                "prefect_server.pollIALiRT.DatetimeProvider.now", side_effect=fake_now
            ),
            patch(
                "prefect_server.pollIALiRT.do_poll_ialirt", return_value=[]
            ) as mock_do_poll,
            patch("prefect_server.pollIALiRT.asyncio.sleep"),
        ):
            await poll_ialirt_flow.fn(
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
                "prefect_server.pollIALiRT.DatetimeProvider.end_of_hour",
                return_value=end_date,
            ),
            patch(
                "prefect_server.pollIALiRT.DatetimeProvider.now", side_effect=fake_now
            ),
            patch(
                "prefect_server.pollIALiRT.do_poll_ialirt_hk", return_value=[]
            ) as mock_do_poll,
            patch("prefect_server.pollIALiRT.asyncio.sleep"),
        ):
            await poll_ialirt_hk_flow.fn(
                end_date=end_date,
                wait_for_new_data_to_arrive=True,
            )

        mock_do_poll.assert_called_once()


class TestPollIALiRTGenerateName:
    def test_name_with_no_dates_uses_last_update(self):
        mock_params = {"start_date": None, "end_date": None}
        with patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run:
            with patch(
                "prefect_server.pollIALiRT.DatetimeProvider.end_of_hour",
                return_value=datetime(2025, 6, 1),
            ):
                mock_flow_run.parameters = mock_params
                name = generate_flow_run_name()

        assert "last-update" in name

    def test_name_with_dates(self):
        mock_params = {
            "start_date": datetime(2025, 6, 1, 12, 0, 0),
            "end_date": datetime(2025, 6, 1, 13, 0, 0),
        }
        with patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name()

        assert "01-06-2025" in name

    def test_hk_name_with_no_dates(self):
        mock_params = {"start_date": None, "end_date": None}
        with patch("prefect_server.pollIALiRT.flow_run") as mock_flow_run:
            with patch(
                "prefect_server.pollIALiRT.DatetimeProvider.end_of_hour",
                return_value=datetime(2025, 6, 1),
            ):
                mock_flow_run.parameters = mock_params
                name = generate_hk_flow_run_name()

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
            )

        assert downloaded_file in result
