import logging
import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from imap_mag.client.WebTCADLaTiS import HKWebTCADItems, WebTCADLaTiS
from imap_mag.config.ApiSource import (
    WebTCADLaTiSApiSource,
)
from imap_mag.config.FetchConfig import (
    FetchWebTCADLaTiSConfig,
)
from imap_mag.util.Subsystem import Subsystem
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

    client = build_client_under_test(wiremock_manager)

    result = client.download_imap_lo_pivot_platform_angle_to_csv_file(
        start_date=start_date,
        end_date=end_date,
    )

    assert result == expected_csv


def build_client_under_test(wiremock_manager):
    return WebTCADLaTiS(
        fetch_webtcad_config=FetchWebTCADLaTiSConfig(
            api=WebTCADLaTiSApiSource(
                url_base=wiremock_manager.get_url(),
                system_id="SID1",
                auth_code=SecretStr("test-auth-code"),
            )
        ),
    )


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

    client = build_client_under_test(wiremock_manager)

    with pytest.raises(Exception):
        client.download_imap_lo_pivot_platform_angle_to_csv_file(
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

    client = build_client_under_test(wiremock_manager)

    result = client.download_imap_lo_pivot_platform_angle_to_csv_file(
        start_date=start_date,
        end_date=end_date,
    )

    assert result == "time,value\n"
    # Only the header, no data
    assert len(result.strip().splitlines()) == 1


def build_client_no_auth():
    """Build a client with no auth code (for validation tests that do not hit the network)."""
    return WebTCADLaTiS(
        fetch_webtcad_config=FetchWebTCADLaTiSConfig(
            api=WebTCADLaTiSApiSource(
                url_base="http://test-server",
                system_id="SID1",
                auth_code=None,
            )
        ),
    )


def test_constructor_warns_when_no_auth_code(caplog):
    """Test that constructor logs a warning when no auth code is provided."""
    with caplog.at_level(logging.WARNING, logger="imap_mag.client.WebTCADLaTiS"):
        build_client_no_auth()

    assert any(
        "No authentication code provided" in record.message for record in caplog.records
    )


def test_download_raises_on_empty_system_id():
    """Test that ValueError is raised when system_id is empty."""
    client = build_client_no_auth()

    with pytest.raises(ValueError, match="System ID must be provided"):
        client.download_analog_telemetry_item(
            telemetry_item_id=58350,
            start_date=datetime(2026, 2, 11),
            end_date=datetime(2026, 2, 12),
            system_id="",
        )


def test_download_raises_on_missing_start_date():
    """Test that ValueError is raised when start_date is None."""
    client = build_client_no_auth()

    with pytest.raises(ValueError, match="Start date and end date must be provided"):
        client.download_analog_telemetry_item(
            telemetry_item_id=58350,
            start_date=None,
            end_date=datetime(2026, 2, 12),
        )


def test_download_raises_on_missing_end_date():
    """Test that ValueError is raised when end_date is None."""
    client = build_client_no_auth()

    with pytest.raises(ValueError, match="Start date and end date must be provided"):
        client.download_analog_telemetry_item(
            telemetry_item_id=58350,
            start_date=datetime(2026, 2, 11),
            end_date=None,
        )


def test_download_raises_on_zero_telemetry_item_id():
    """Test that ValueError is raised when telemetry_item_id is 0 (falsy)."""
    client = build_client_no_auth()

    with pytest.raises(ValueError, match="Telemetry item ID must be provided"):
        client.download_analog_telemetry_item(
            telemetry_item_id=0,
            start_date=datetime(2026, 2, 11),
            end_date=datetime(2026, 2, 12),
        )


def test_download_analog_telemetry_with_ert_mode_and_json_format():
    """Test that download_analog_telemetry_item uses ERT mode and JSON format in URL."""
    client = build_client_no_auth()

    mock_response = MagicMock()
    mock_response.text = '{"data": []}'
    mock_response.content = b'{"data": []}'
    mock_response.raise_for_status = MagicMock()

    with patch("requests.get", return_value=mock_response) as mock_get:
        result = client.download_analog_telemetry_item(
            telemetry_item_id=58350,
            start_date=datetime(2026, 2, 11),
            end_date=datetime(2026, 2, 12),
            mode=WebTCADLaTiS.TimeQueryMode.EARTH_RECEIVED_TIME_MODE,
            results_format=WebTCADLaTiS.ResultsFormat.JSON,
        )

    assert result == '{"data": []}'
    call_url = mock_get.call_args[0][0]
    assert "_ERT_SID1.json" in call_url


def test_download_analog_telemetry_constructs_correct_url():
    """Test that the constructed URL for a telemetry download is correct."""
    client = build_client_no_auth()

    mock_response = MagicMock()
    mock_response.text = "time,value\n"
    mock_response.content = b"time,value\n"
    mock_response.raise_for_status = MagicMock()

    with patch("requests.get", return_value=mock_response) as mock_get:
        client.download_analog_telemetry_item(
            telemetry_item_id=12345,
            start_date=datetime(2026, 3, 1, 0, 0, 0),
            end_date=datetime(2026, 3, 2, 0, 0, 0),
            system_id="SID2",
        )

    call_url = mock_get.call_args[0][0]
    assert "TMID=12345" in call_url
    assert "_SID2.csv" in call_url
    assert "2026-03-01T00:00:00.000Z" in call_url
    assert "2026-03-02T00:00:00.000Z" in call_url


def test_hk_webtcad_items_enum_values():
    """Test that HKWebTCADItems enum has correct attributes."""
    item = HKWebTCADItems.LO_PIVOT_PLATFORM_ANGLE

    assert item.tmid == 58350
    assert item.packet_name == "ILOGLOBAL.PPM_NHK_POT_PRI"
    assert item.instrument == Subsystem.LO
    assert item.descriptor == "pivot-platform-angle"
    # Enum value is the name (as set in __init__)
    assert item.value == "LO_PIVOT_PLATFORM_ANGLE"
