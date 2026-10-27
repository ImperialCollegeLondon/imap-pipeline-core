"""Unit tests for pollSpice flow progress options."""

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

from prefect_server.pollSpice import poll_spice_flow

DATES = {"ingest_start_day": date(2025, 6, 1), "ingest_end_date": date(2025, 6, 2)}


@pytest.mark.parametrize(
    "params, expect_progress_validation, expect_progress_saved",
    [
        ({}, True, True),
        (DATES, True, False),
        ({**DATES, "force_redownload": True}, False, False),
        ({**DATES, "overwrite_download_progress_date": True}, True, True),
        (
            {
                **DATES,
                "force_redownload": True,
                "overwrite_download_progress_date": True,
            },
            False,
            True,
        ),
    ],
    ids=[
        "automated",
        "range_respects_progress",
        "range_force_redownload",
        "range_overwrite_progress",
        "range_force_redownload_and_overwrite_progress",
    ],
)
@pytest.mark.asyncio
async def test_poll_spice_flow_progress_options(
    params, expect_progress_validation, expect_progress_saved
):
    mock_dm = MagicMock()
    mock_dm.get_dates_for_download.return_value = (
        datetime(2025, 6, 1),
        datetime(2025, 6, 2),
    )

    with (
        patch("prefect_server.pollSpice.try_get_prefect_logger"),
        patch("prefect_server.pollSpice.Database"),
        patch("prefect_server.pollSpice.get_secret_or_env_var", return_value="code"),
        patch("prefect_server.pollSpice.DownloadDateManager", return_value=mock_dm),
        patch("prefect_server.pollSpice.fetch_spice", return_value=[]),
        patch("prefect_server.pollSpice.update_database_with_progress") as mock_update,
    ):
        await poll_spice_flow.fn(**params)

    assert (
        mock_dm.get_dates_for_download.call_args.kwargs["validate_with_database"]
        == expect_progress_validation
    )
    assert mock_update.called == expect_progress_saved
