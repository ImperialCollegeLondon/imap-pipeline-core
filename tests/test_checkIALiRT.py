import re
import sys
from datetime import timedelta
from unittest import mock

import pytest
from prefect.exceptions import FailedRun

from imap_mag.util import DatetimeProvider
from prefect_server.checkIALiRT import check_ialirt_flow
from prefect_server.constants import PREFECT_CONSTANTS
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    NOW,
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
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    capture_cli_logs,
):
    # Exercise.
    await check_ialirt_flow(files=[TEST_DATA / "ialirt_data_without_anomalies.csv"])

    # Verify.
    assert "No anomalies detected in I-ALiRT data." in capture_cli_logs.text

    ialirt_monthly_test = test_database.get_workflow_progress(
        PREFECT_CONSTANTS.CHECK_IALIRT.IALIRT_MONTHLY_TEST_WORKFLOW_NAME
    )

    assert ialirt_monthly_test.get_progress_timestamp() is None
    assert ialirt_monthly_test.get_last_checked_date() == NOW


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
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    capture_cli_logs,
):
    # Exercise.
    await check_ialirt_flow()

    # Verify.
    assert f"Loading I-ALiRT data from {YESTERDAY} to {TODAY}." in capture_cli_logs.text
    assert "No anomalies detected in I-ALiRT data." in capture_cli_logs.text

    ialirt_monthly_test = test_database.get_workflow_progress(
        PREFECT_CONSTANTS.CHECK_IALIRT.IALIRT_MONTHLY_TEST_WORKFLOW_NAME
    )

    assert ialirt_monthly_test.get_progress_timestamp() is None
    assert ialirt_monthly_test.get_last_checked_date() == NOW


# Force the next tests to be at first Monday of the month
NOW_FIRST_MONDAY_OF_MONTH = NOW.replace(
    year=2025,
    month=11,
    day=3,
)


@pytest.fixture(autouse=False)
def mock_datetime_provider_first_monday_of_month(monkeypatch):
    monkeypatch.setattr(DatetimeProvider, "now", lambda: NOW_FIRST_MONDAY_OF_MONTH)


@pytest.mark.asyncio
async def test_check_ialirt_first_monday_of_month_first_time(
    temp_datastore,  # noqa: F811
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider_first_monday_of_month,
    mock_teams_webhook_block,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Exercise.
    await check_ialirt_flow(files=[TEST_DATA / "ialirt_data_without_anomalies.csv"])

    # Verify.
    assert "No anomalies detected in I-ALiRT data." in capture_cli_logs.text

    ialirt_monthly_test = test_database.get_workflow_progress(
        PREFECT_CONSTANTS.CHECK_IALIRT.IALIRT_MONTHLY_TEST_WORKFLOW_NAME
    )

    assert ialirt_monthly_test.get_progress_timestamp() == NOW_FIRST_MONDAY_OF_MONTH
    assert ialirt_monthly_test.get_last_checked_date() == NOW_FIRST_MONDAY_OF_MONTH

    mock_teams_webhook_block.notify.assert_called_once()


@pytest.mark.asyncio
async def test_check_ialirt_first_monday_of_month_not_first_time(
    temp_datastore,  # noqa: F811
    test_database,  # noqa: F811
    prefect_test_fixture,  # noqa: F811
    mock_datetime_provider_first_monday_of_month,
    mock_teams_webhook_block,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Set up.
    ialirt_monthly_test = test_database.get_workflow_progress(
        PREFECT_CONSTANTS.CHECK_IALIRT.IALIRT_MONTHLY_TEST_WORKFLOW_NAME
    )
    previous_progress_timestamp = NOW_FIRST_MONDAY_OF_MONTH - timedelta(seconds=10)

    ialirt_monthly_test.update_progress_timestamp(previous_progress_timestamp)
    test_database.save(ialirt_monthly_test)

    # Exercise.
    await check_ialirt_flow(files=[TEST_DATA / "ialirt_data_without_anomalies.csv"])

    # Verify.
    assert "No anomalies detected in I-ALiRT data." in capture_cli_logs.text

    ialirt_monthly_test = test_database.get_workflow_progress(
        PREFECT_CONSTANTS.CHECK_IALIRT.IALIRT_MONTHLY_TEST_WORKFLOW_NAME
    )

    assert ialirt_monthly_test.get_progress_timestamp() == previous_progress_timestamp
    assert ialirt_monthly_test.get_last_checked_date() == NOW_FIRST_MONDAY_OF_MONTH

    mock_teams_webhook_block.notify.assert_not_called()
