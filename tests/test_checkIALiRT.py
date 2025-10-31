import re
import sys
from unittest import mock

import pytest
from prefect.exceptions import FailedRun

from prefect_server.checkIALiRT import check_ialirt_flow
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    TEST_DATA,
    TODAY,
    YESTERDAY,
    mock_datetime_provider,  # noqa: F401
    temp_datastore,  # noqa: F401
)
from tests.util.prefect import (  # noqa: F401
    mock_teams_webhook_block,
    prefect_test_fixture,
)


@pytest.mark.asyncio
async def test_check_ialirt_no_issues(
    temp_datastore,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    capture_cli_logs,
):
    # Exercise.
    await check_ialirt_flow(files=[TEST_DATA / "ialirt_data_without_anomalies.csv"])

    # Verify.
    assert "No anomalies detected in I-ALiRT data." in capture_cli_logs.text


@pytest.mark.skipif(sys.version_info < (3, 13), reason="Requires python3.13 or higher")
@pytest.mark.asyncio
async def test_check_ialirt_with_issues(
    mock_teams_webhook_block: mock.Mock,  # noqa: F811
    temp_datastore,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    capture_cli_logs,
):
    # Exercise.
    with pytest.raises(
        FailedRun, match=re.escape("Anomalies detected in I-ALiRT data.")
    ):
        await check_ialirt_flow(files=[TEST_DATA / "ialirt_data_with_anomalies.csv"])

    # Verify.
    assert "Detected 7 anomalies in I-ALiRT data:" in capture_cli_logs.text

    mock_teams_webhook_block.notify.assert_called()
    assert mock_teams_webhook_block.notify.call_count == 7


@pytest.mark.asyncio
async def test_check_ialirt_no_files_default_dates(
    temp_datastore,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    capture_cli_logs,
):
    # Exercise.
    await check_ialirt_flow()

    # Verify.
    assert f"Loading I-ALiRT data from {YESTERDAY} to {TODAY}." in capture_cli_logs.text
    assert "No anomalies detected in I-ALiRT data." in capture_cli_logs.text
