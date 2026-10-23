"""Tests for `IALiRTDataAccess` class."""

import json
import os
from contextlib import nullcontext as does_not_raise

import pytest

from imap_mag.client.NOAAApiClient import NOAARTSWApiClient
from tests.util.miscellaneous import temp_datastore  # noqa: F401


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.parametrize(
    "spacecraft,instrument,records,expected_key,raises",
    [
        pytest.param(
            "SOLAR1", "mag", 2, "mag_B_GSE", does_not_raise(), id="SOLAR-1, mag, valid"
        ),
        pytest.param(
            "SOLAR1",
            "wind",
            2,
            "speed",
            does_not_raise(),
            id="SOLAR-1, wind, valid",
        ),
        pytest.param(
            "ACE", "mag", 1, "mag_B_GSE", does_not_raise(), id="ACE, mag, valid"
        ),
        pytest.param(
            "ACE", "wind", 1, "speed", does_not_raise(), id="ACE, wind, valid"
        ),
        pytest.param(
            "ENTERPRISE",
            "mag",
            2,
            "mag_B_GSE",
            pytest.raises(
                ValueError,
                match=r"Invalid spacecraft requested. "
                "It must be 'SOLAR1' or 'ACE', but 'ENTERPRISE' found",
            ),
            id="Invalid spacecraft",
        ),
        pytest.param(
            "ACE",
            "flux",
            1,
            "mag_B_GSE",
            pytest.raises(
                ValueError,
                match=r"Invalid instrument type requested for ACE. "
                "It must be 'mag' or 'wind', but 'flux' found",
            ),
            id="Invalid spacecraft",
        ),
    ],
)
def test_download_rtsw_data(
    spacecraft,
    instrument,
    records,
    expected_key,
    raises,
    wiremock_manager,
    capture_cli_logs,
) -> None:
    # Set up.
    response_mag: list[dict] = [
        {
            "source": "SOLAR1",
            "mag_B_GSE": [-1.53, -3.033, 0.539],
        },
        {
            "source": "SOLAR1",
            "mag_B_GSE": [4.187, 0.687, 0.757],
        },
        {
            "source": "ACE",
            "mag_B_GSE": [0.596, 3.986, -1.569],
        },
    ]
    response_wind: list[dict] = [
        {
            "source": "SOLAR1",
            "speed": 400.0,
        },
        {
            "source": "SOLAR1",
            "speed": 500.0,
        },
        {
            "source": "ACE",
            "speed": 600.0,
        },
    ]

    wiremock_manager.add_string_mapping(
        "/rtsw_mag_1m.json",
        json.dumps(response_mag),
    )
    wiremock_manager.add_string_mapping(
        "/rtsw_wind_1m.json",
        json.dumps(response_wind),
    )

    data_access = NOAARTSWApiClient(url=wiremock_manager.get_url().rstrip("/"))

    with raises:
        data = data_access.get_data(spacecraft, instrument)
        assert len(data) == records
        assert expected_key in data[0]

        assert (
            f"Downloaded {len(data)} {instrument} records for {spacecraft}"
            in capture_cli_logs.text
        )
