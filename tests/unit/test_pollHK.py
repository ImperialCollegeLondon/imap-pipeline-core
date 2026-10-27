"""Unit tests for pollHK flow name generation."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from imap_mag.util import HKPacket
from prefect_server.pollHK import generate_flow_run_name, poll_hk_flow


class TestPollHKFlowGenerateName:
    def test_auto_run_includes_last_update(self):
        mock_params = {
            "hk_packets": list(HKPacket),
            "start_date": None,
            "end_date": None,
        }
        with patch("prefect_server.pollHK.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name()

        assert "last-update" in name
        assert "all-HK" in name

    def test_specific_dates_included_in_name(self):
        mock_params = {
            "hk_packets": [HKPacket.SID1],
            "start_date": datetime(2025, 6, 1),
            "end_date": datetime(2025, 6, 30),
        }
        with patch("prefect_server.pollHK.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name()

        assert "01-06-2025" in name


DATES = {"start_date": datetime(2025, 6, 1), "end_date": datetime(2025, 6, 2)}


@pytest.mark.parametrize(
    "params, expect_progress_validation, expect_progress_saved",
    [
        ({}, True, True),
        ({**DATES, "force_ert": True}, True, False),
        ({**DATES, "force_ert": True, "force_redownload": True}, False, False),
        (
            {**DATES, "force_ert": True, "overwrite_download_progress_date": True},
            True,
            True,
        ),
        (
            {
                **DATES,
                "force_ert": True,
                "force_redownload": True,
                "overwrite_download_progress_date": True,
            },
            False,
            True,
        ),
        (DATES, False, False),
        ({**DATES, "overwrite_download_progress_date": True}, False, False),
    ],
    ids=[
        "automated",
        "ert_range_respects_progress",
        "ert_range_force_redownload",
        "ert_range_overwrite_progress",
        "ert_range_force_redownload_and_overwrite_progress",
        "spacecraft_time_range_ignores_progress",
        "spacecraft_time_range_never_saves_progress",
    ],
)
@pytest.mark.asyncio
async def test_poll_hk_flow_progress_options(
    params, expect_progress_validation, expect_progress_saved
):
    mock_dm = MagicMock()
    mock_dm.get_dates_for_download.return_value = (
        datetime(2025, 6, 1),
        datetime(2025, 6, 2),
    )

    with (
        patch("prefect_server.pollHK.try_get_prefect_logger"),
        patch("prefect_server.pollHK.Database"),
        patch("prefect_server.pollHK.get_secret_or_env_var", return_value="code"),
        patch("prefect_server.pollHK.DownloadDateManager", return_value=mock_dm),
        patch("prefect_server.pollHK.fetch_binary", return_value={}),
        patch("prefect_server.pollHK.update_database_with_progress") as mock_update,
    ):
        await poll_hk_flow.fn(hk_packets=[HKPacket.SID1], **params)

    assert (
        mock_dm.get_dates_for_download.call_args.kwargs["validate_with_database"]
        == expect_progress_validation
    )
    assert mock_update.called == expect_progress_saved
