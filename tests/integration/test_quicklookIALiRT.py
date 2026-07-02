import os
import shutil
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from prefect_server.quicklookIALiRT import generate_flow_run_name, quicklook_ialirt_flow
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    TEST_DATA,
    temp_datastore,  # noqa: F401
)
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401


class TestQuicklookIALiRTFlowUnit:
    def test_generate_flow_run_name_uses_dates(self):
        mock_params = {
            "start_date": datetime(2025, 6, 1),
            "end_date": datetime(2025, 6, 2),
        }
        with patch("prefect_server.quicklookIALiRT.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name()

        assert "2025" in name

    @pytest.mark.asyncio
    async def test_quicklook_ialirt_flow_calls_plot_ialirt(self):
        mock_plot = MagicMock()

        with patch("prefect_server.quicklookIALiRT.plot_ialirt", mock_plot):
            await quicklook_ialirt_flow.fn(
                start_date=datetime(2025, 6, 1),
                end_date=datetime(2025, 6, 2),
            )

        mock_plot.assert_called_once()


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_ialirt_autoflow_first_ever_run(
    wiremock_manager,
    temp_datastore,  # noqa: F811
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
):
    # Set up.
    wiremock_manager.reset()

    science_data = TEST_DATA / "ialirt_science_plot_data.csv"
    hk_data = TEST_DATA / "ialirt_hk_plot_data.csv"

    (temp_datastore / "ialirt" / "2025" / "10").mkdir(parents=True, exist_ok=True)
    shutil.copy(
        science_data,
        temp_datastore / "ialirt" / "2025" / "10" / "imap_ialirt_mag_20251021.csv",
    )

    (temp_datastore / "ialirt" / "2025" / "10").mkdir(parents=True, exist_ok=True)
    shutil.copy(
        hk_data,
        temp_datastore / "ialirt" / "2025" / "10" / "imap_ialirt_mag_hk_20251021.csv",
    )

    # Exercise.
    await quicklook_ialirt_flow(
        start_date=datetime(2025, 10, 21), end_date=datetime(2025, 10, 21, 23, 59, 59)
    )

    # Verify.
    assert (
        temp_datastore
        / "quicklook"
        / "ialirt"
        / "2025"
        / "10"
        / "imap_quicklook_ialirt_20251021.png"
    ).exists()
