import os
from datetime import datetime

import pytest
from pydantic import SecretStr

from imap_mag.client.WebTCADLaTiS import WebTCADLaTiS
from tests.util.miscellaneous import mock_datetime_provider  # noqa: F401


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_download_csv(wiremock_manager):
    """Test that WebTCADLaTiS correctly constructs the URL and returns CSV data."""
    wiremock_manager.reset()

    expected_csv = "time,value\n2026-02-11T14:00:00.000,45.123\n"

    start_date = datetime(2026, 2, 11)
    end_date = datetime(2026, 2, 12)

    url = (
        "/AnalogTelemetryItem_SID1.csv"
        "?TMID=58350"
        "&time,value"
        "&time%3E=2026-02-11T00:00:00.000Z"
        "&time%3C=2026-02-12T00:00:00.000Z"
        "&format_time(yyyy-MM-dd'T'HH:mm:ss.SSS)"
    )

    wiremock_manager.add_string_mapping(url, expected_csv, priority=1)

    client = WebTCADLaTiS(
        auth_code=SecretStr("test-auth-code"),
        base_url=wiremock_manager.get_url(),
    )

    result = client.download_csv(
        tmid=58350,
        start_date=start_date,
        end_date=end_date,
    )

    assert result == expected_csv


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_download_csv_raises_on_error(wiremock_manager):
    """Test that WebTCADLaTiS raises on HTTP error."""
    wiremock_manager.reset()

    start_date = datetime(2026, 2, 11)
    end_date = datetime(2026, 2, 12)

    url = (
        "/AnalogTelemetryItem_SID1.csv"
        "?TMID=58350"
        "&time,value"
        "&time%3E=2026-02-11T00:00:00.000Z"
        "&time%3C=2026-02-12T00:00:00.000Z"
        "&format_time(yyyy-MM-dd'T'HH:mm:ss.SSS)"
    )

    wiremock_manager.add_string_mapping(url, "Unauthorized", status=401, priority=1)

    client = WebTCADLaTiS(
        auth_code=SecretStr("bad-auth-code"),
        base_url=wiremock_manager.get_url(),
    )

    with pytest.raises(Exception):
        client.download_csv(
            tmid=58350,
            start_date=start_date,
            end_date=end_date,
        )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_download_csv_empty_response(wiremock_manager):
    """Test that WebTCADLaTiS returns empty CSV content."""
    wiremock_manager.reset()

    start_date = datetime(2026, 2, 11)
    end_date = datetime(2026, 2, 12)

    url = (
        "/AnalogTelemetryItem_SID1.csv"
        "?TMID=58350"
        "&time,value"
        "&time%3E=2026-02-11T00:00:00.000Z"
        "&time%3C=2026-02-12T00:00:00.000Z"
        "&format_time(yyyy-MM-dd'T'HH:mm:ss.SSS)"
    )

    wiremock_manager.add_string_mapping(url, "time,value\n", priority=1)

    client = WebTCADLaTiS(
        auth_code=SecretStr("test-auth-code"),
        base_url=wiremock_manager.get_url(),
    )

    result = client.download_csv(
        tmid=58350,
        start_date=start_date,
        end_date=end_date,
    )

    assert result == "time,value\n"
    # Only the header, no data
    assert len(result.strip().splitlines()) == 1
