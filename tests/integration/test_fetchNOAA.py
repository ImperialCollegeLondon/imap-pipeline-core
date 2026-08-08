import json
import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from imap_mag.client.NOAAApiClient import NOAARTSWApiClient
from imap_mag.download.FetchNOAA import FetchNOAA
from imap_mag.io import FileFinder

NOAA_DATA_PATH = Path(__file__).parent.parent / "datastore" / "noaa"


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.parametrize(
    "spacecraft,instrument",
    [
        pytest.param("SOLAR1", "mag", id="SOLAR-1, mag"),
        pytest.param("SOLAR1", "wind", id="SOLAR-1, wind"),
        pytest.param("ACE", "mag", id="ACE, mag"),
        pytest.param("ACE", "wind", id="ACE, wind"),
    ],
)
def test_fetch_rtsw_data(
    spacecraft,
    instrument,
    wiremock_manager,
    capture_cli_logs,
) -> None:
    # Set up.
    filename = f"rtsw_{instrument}_1m.json"
    mock_data_path = NOAA_DATA_PATH / filename
    with open(mock_data_path) as f:
        response = json.load(f)
    wiremock_manager.add_string_mapping(
        f"/{filename}",
        json.dumps(response),
    )

    # Create FetchNOAA instance.
    work_folder = Path(tempfile.mkdtemp())
    datastore_finder = FileFinder(Path(tempfile.mkdtemp()))
    fetch = FetchNOAA(
        data_access=NOAARTSWApiClient(url=wiremock_manager.get_url().rstrip("/")),
        work_folder=work_folder,
        datastore_finder=datastore_finder,
    )

    # Download data.
    downloaded_files = fetch.download_csv(spacecraft, instrument)

    # Validate downloaded files.
    assert len(downloaded_files) == 2
    for file_path, path_handler in downloaded_files.items():
        assert file_path.exists()
        assert path_handler is not None

        expected_filename = (
            f"{spacecraft}_{instrument}_noaa_{file_path.stem.split('_')[-1]}.csv"
        )
        assert file_path.name == expected_filename

        date_str = file_path.stem.split("_")[-1]
        year = date_str[:4]
        month = date_str[4:6]
        expected_data = pd.read_csv(NOAA_DATA_PATH / year / month / expected_filename)
        actual_data = pd.read_csv(file_path)
        pd.testing.assert_frame_equal(expected_data, actual_data)
