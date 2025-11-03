import os
import shutil
from datetime import datetime

import pytest

from prefect_server.quicklookIALiRT import quicklook_ialirt_flow
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    TEST_DATA,
    temp_datastore,  # noqa: F401
)
from tests.util.prefect import prefect_test_fixture  # noqa: F401


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

    test_data = TEST_DATA / "ialirt_plot_data.csv"

    (temp_datastore / "ialirt" / "2025" / "10").mkdir(parents=True, exist_ok=True)
    shutil.copy(
        test_data,
        temp_datastore / "ialirt" / "2025" / "10" / "imap_ialirt_20251021.csv",
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
