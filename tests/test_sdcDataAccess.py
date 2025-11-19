"""Tests for `SDCDataAccess` class."""

import json
import os
from datetime import date, datetime
from pathlib import Path

import imap_data_access
import pytest
from pydantic import SecretStr

from imap_mag.client.SDCDataAccess import SDCDataAccess
from imap_mag.util import Environment


def test_sdc_data_access_constructor_sets_config() -> None:
    # Set up.
    auth_code = SecretStr("some_auth_code")
    data_dir = Path("some_test_folder")
    data_access_url = "https://some_test_url"

    # Exercise.
    _ = SDCDataAccess(auth_code, data_dir, data_access_url)

    # Verify.
    assert imap_data_access.config["API_KEY"] == auth_code.get_secret_value()
    assert imap_data_access.config["DATA_DIR"] == data_dir
    assert imap_data_access.config["DATA_ACCESS_URL"] == data_access_url


def test_get_file_path_builds_file_path() -> None:
    # Set up.
    data_access = SDCDataAccess(
        SecretStr("some_auth_code"), Path("some_test_folder"), "https://some_test_url"
    )

    # Exercise.
    (filename, file_path) = data_access.get_file_path(
        level="l1b",
        descriptor="norm-magi",
        start_date=datetime(2025, 5, 2),
        version="v002",
    )

    # Verify.
    assert filename == Path("imap_mag_l1b_norm-magi_20250502_v002.cdf")
    assert file_path == Path(
        os.path.join("some_test_folder", "imap", "mag", "l1b", "2025", "05", filename)
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_spice_query(wiremock_manager) -> None:
    """Test that spice_query method correctly processes response from API."""
    # Set up.
    # Sample SPICE data from the spec - using a subset for testing
    sample_spice_data = get_sample_spice_data()

    wiremock_manager.reset()

    # Configure wiremock to return SPICE data for the query
    wiremock_manager.add_string_mapping(
        "/spice-query?start_ingest_date=20251101",
        json.dumps(sample_spice_data),
        priority=1,
    )

    # Exercise.
    # Configure SDCDataAccess to use wiremock server
    data_access = SDCDataAccess(
        auth_code=SecretStr("test_token"),
        data_dir=Path("/tmp/test_data"),
        sdc_url=wiremock_manager.get_url(),
    )

    result = data_access.spice_query(ingest_start_day=date(2025, 11, 1))

    # Verify.
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 3

    # Verify first entry
    assert result[0]["file_name"] == "ck/imap_2025_302_2025_303_001.ah.bc"
    assert result[0]["kernel_type"] == "attitude_history"
    assert result[0]["version"] == 1
    assert result[0]["ingestion_date"] == "2025-11-01, 08:05:12"

    # Verify second entry
    assert result[1]["file_name"] == "ck/imap_2025_303_2025_304_001.ah.bc"
    assert result[1]["kernel_type"] == "attitude_history"

    # Verify third entry
    assert result[2]["file_name"] == "sclk/imap_sclk_0032.tsc"
    assert result[2]["kernel_type"] == "spacecraft_clock"
    assert result[2]["version"] == 32


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_spice_query_with_type_and_time_range(wiremock_manager) -> None:
    """Test spice_query method with kernel type and time range parameters."""
    # Set up.
    sample_spice_data = [
        {
            "file_name": "sclk/imap_sclk_0031.tsc",
            "file_root": "imap_sclk_.tsc",
            "kernel_type": "spacecraft_clock",
            "version": 31,
            "min_date_j2000": 315576066.1839245,
            "max_date_j2000": 4575787269.183866,
            "file_intervals_j2000": [[315576066.1839245, 4575787269.183866]],
            "min_date_datetime": "2010-01-01, 00:00:00",
            "max_date_datetime": "2145-01-01, 00:00:00",
            "file_intervals_datetime": [["2010-01-01T00:00:00", "2145-01-01T00:00:00"]],
            "min_date_sclk": "1/0000000000:00000",
            "max_date_sclk": "1/4260214609:46809",
            "file_intervals_sclk": [["1/0000000000:00000", "1/4260214609:46809"]],
            "sclk_kernel": "/tmp/naif0012.tls",
            "lsk_kernel": "/tmp/imap_sclk_0030.tsc",
            "ingestion_date": "2025-10-30, 08:10:11",
            "timestamp": 1761811811.0,
        },
        {
            "file_name": "sclk/imap_sclk_0032.tsc",
            "file_root": "imap_sclk_.tsc",
            "kernel_type": "spacecraft_clock",
            "version": 32,
            "min_date_j2000": 315576066.1839245,
            "max_date_j2000": 4575787269.183866,
            "file_intervals_j2000": [[315576066.1839245, 4575787269.183866]],
            "min_date_datetime": "2010-01-01, 00:00:00",
            "max_date_datetime": "2145-01-01, 00:00:00",
            "file_intervals_datetime": [["2010-01-01T00:00:00", "2145-01-01T00:00:00"]],
            "min_date_sclk": "1/0000000000:00000",
            "max_date_sclk": "1/4260214608:42276",
            "file_intervals_sclk": [["1/0000000000:00000", "1/4260214608:42276"]],
            "sclk_kernel": "/tmp/naif0012.tls",
            "lsk_kernel": "/tmp/imap_sclk_0031.tsc",
            "ingestion_date": "2025-11-01, 08:10:08",
            "timestamp": 1761984608.0,
        },
    ]

    wiremock_manager.reset()

    # Configure wiremock to return SPICE data for the query with type and time range
    # Note: Parameter order matters - they are added in the order specified in the method
    wiremock_manager.add_string_mapping(
        "/spice-query?start_time=1761984600&end_time=1761984610&type=spacecraft_clock",
        json.dumps(sample_spice_data),
        priority=1,
    )

    # Exercise.
    # Configure SDCDataAccess to use wiremock server
    data_access = SDCDataAccess(
        auth_code=SecretStr("test_token"),
        data_dir=Path("/tmp/test_data"),
        sdc_url=wiremock_manager.get_url(),
    )

    result = data_access.spice_query(
        kernel_type="spacecraft_clock", start_time=1761984600, end_time=1761984610
    )

    # Verify.
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 2

    # Verify both are spacecraft_clock kernels
    for item in result:
        assert item["kernel_type"] == "spacecraft_clock"
        assert item["file_root"] == "imap_sclk_.tsc"

    # Verify versions
    assert result[0]["version"] == 31
    assert result[1]["version"] == 32


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_spice_query_with_latest_flag(wiremock_manager) -> None:
    """Test spice_query method with latest flag to get only newest version."""
    # Set up.
    # When latest=True, API should only return version 32
    sample_spice_data = [
        {
            "file_name": "sclk/imap_sclk_0032.tsc",
            "file_root": "imap_sclk_.tsc",
            "kernel_type": "spacecraft_clock",
            "version": 32,
            "min_date_j2000": 315576066.1839245,
            "max_date_j2000": 4575787269.183866,
            "file_intervals_j2000": [[315576066.1839245, 4575787269.183866]],
            "min_date_datetime": "2010-01-01, 00:00:00",
            "max_date_datetime": "2145-01-01, 00:00:00",
            "file_intervals_datetime": [["2010-01-01T00:00:00", "2145-01-01T00:00:00"]],
            "min_date_sclk": "1/0000000000:00000",
            "max_date_sclk": "1/4260214608:42276",
            "file_intervals_sclk": [["1/0000000000:00000", "1/4260214608:42276"]],
            "sclk_kernel": "/tmp/naif0012.tls",
            "lsk_kernel": "/tmp/imap_sclk_0031.tsc",
            "ingestion_date": "2025-11-01, 08:10:08",
            "timestamp": 1761984608.0,
        }
    ]

    wiremock_manager.reset()

    # Configure wiremock to return only latest version
    # Note: imap_data_access adds / prefix to the base URL
    # Note: Parameter order matters - they are added in the order specified in the method
    wiremock_manager.add_string_mapping(
        "/spice-query?start_time=1761984600&end_time=1761984610&type=spacecraft_clock&latest=True",
        json.dumps(sample_spice_data),
        priority=1,
    )

    # Exercise.
    # Configure SDCDataAccess to use wiremock server
    data_access = SDCDataAccess(
        auth_code=SecretStr("test_token"),
        data_dir=Path("/tmp/test_data"),
        sdc_url=wiremock_manager.get_url(),
    )

    result = data_access.spice_query(
        kernel_type="spacecraft_clock",
        start_time=1761984600,
        end_time=1761984610,
        latest=True,
    )

    # Verify.
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 1

    # Verify only latest version (32) is returned
    assert result[0]["version"] == 32
    assert result[0]["kernel_type"] == "spacecraft_clock"
    assert result[0]["file_name"] == "sclk/imap_sclk_0032.tsc"


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_spice_query_with_date_range(wiremock_manager) -> None:
    """Test spice_query method with both start and end dates."""
    # Set up.
    sample_spice_data = get_sample_spice_data()

    wiremock_manager.reset()

    # Configure wiremock to return SPICE data for the query with date range
    wiremock_manager.add_string_mapping(
        "/spice-query?start_ingest_date=20251101&end_ingest_date=20251105",
        json.dumps(sample_spice_data),
        priority=1,
    )

    # Exercise.
    # Configure SDCDataAccess to use wiremock server
    data_access = SDCDataAccess(
        auth_code=SecretStr("test_token"),
        data_dir=Path("/tmp/test_data"),
        sdc_url=wiremock_manager.get_url(),
    )

    result = data_access.spice_query(
        ingest_start_day=date(2025, 11, 1), ingest_end_date=date(2025, 11, 5)
    )

    # Verify.
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 3

    # Verify the query returns expected kernel types
    kernel_types = [item["kernel_type"] for item in result]
    assert "attitude_history" in kernel_types
    assert "spacecraft_clock" in kernel_types

    # Verify all entries have required fields
    for item in result:
        assert "file_name" in item
        assert "kernel_type" in item
        assert "version" in item
        assert "ingestion_date" in item


def get_sample_spice_data():
    return [
        {
            "file_name": "ck/imap_2025_302_2025_303_001.ah.bc",
            "file_root": "imap_2025_302_2025_303_.ah.bc",
            "kernel_type": "attitude_history",
            "version": 1,
            "min_date_j2000": 815036897.0909909,
            "max_date_j2000": 815126896.0094784,
            "file_intervals_j2000": [[815036897.0909909, 815126896.0094784]],
            "min_date_datetime": "2025-10-29, 19:07:07",
            "max_date_datetime": "2025-10-30, 20:07:06",
            "file_intervals_datetime": [
                ["2025-10-29T19:07:07.908503+00:00", "2025-10-30T20:07:06.826978+00:00"]
            ],
            "min_date_sclk": "1/0499460830:00000",
            "max_date_sclk": "1/0499550829:00000",
            "file_intervals_sclk": [["1/0499460830:00000", "1/0499550829:00000"]],
            "sclk_kernel": "/tmp/naif0012.tls",
            "lsk_kernel": "/tmp/imap_sclk_0031.tsc",
            "ingestion_date": "2025-11-01, 08:05:12",
            "timestamp": 1761984312.0,
        },
        {
            "file_name": "ck/imap_2025_303_2025_304_001.ah.bc",
            "file_root": "imap_2025_303_2025_304_.ah.bc",
            "kernel_type": "attitude_history",
            "version": 1,
            "min_date_j2000": 815123297.0127382,
            "max_date_j2000": 815193196.949428,
            "file_intervals_j2000": [[815123297.0127382, 815193196.949428]],
            "min_date_datetime": "2025-10-30, 19:07:07",
            "max_date_datetime": "2025-10-31, 14:32:07",
            "file_intervals_datetime": [
                ["2025-10-30T19:07:07.830239+00:00", "2025-10-31T14:32:07.766918+00:00"]
            ],
            "min_date_sclk": "1/0499547230:00000",
            "max_date_sclk": "1/0499617130:00000",
            "file_intervals_sclk": [["1/0499547230:00000", "1/0499617130:00000"]],
            "sclk_kernel": "/tmp/naif0012.tls",
            "lsk_kernel": "/tmp/imap_sclk_0031.tsc",
            "ingestion_date": "2025-11-01, 08:05:13",
            "timestamp": 1761984313.0,
        },
        {
            "file_name": "sclk/imap_sclk_0032.tsc",
            "file_root": "imap_sclk_.tsc",
            "kernel_type": "spacecraft_clock",
            "version": 32,
            "min_date_j2000": 315576066.1839245,
            "max_date_j2000": 4575787269.183866,
            "file_intervals_j2000": [[315576066.1839245, 4575787269.183866]],
            "min_date_datetime": "2010-01-01, 00:00:00",
            "max_date_datetime": "2145-01-01, 00:00:00",
            "file_intervals_datetime": [["2010-01-01T00:00:00", "2145-01-01T00:00:00"]],
            "min_date_sclk": "1/0000000000:00000",
            "max_date_sclk": "1/4260214608:42276",
            "file_intervals_sclk": [["1/0000000000:00000", "1/4260214608:42276"]],
            "sclk_kernel": "/tmp/naif0012.tls",
            "lsk_kernel": "/tmp/imap_sclk_0031.tsc",
            "ingestion_date": "2025-11-01, 08:10:08",
            "timestamp": 1761984608.0,
        },
    ]


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_spice_download(wiremock_manager, temp_folder_path):
    """Test downloading SPICE files."""
    wiremock_manager.reset()

    # Setup query response
    sample_data = get_sample_spice_data()
    # Return only the spacecraft_clock kernel
    spice_kernel = sample_data[2]

    wiremock_manager.add_string_mapping(
        "/spice-query?start_ingest_date=20251101",
        json.dumps([spice_kernel]),
        priority=1,
    )

    # Setup download response - return the actual test SPICE file
    # Note: imap_data_access adds /imap/spice/ prefix to the download path
    spice_file_path = (
        Path(__file__).parent / "test_data" / "spice" / "imap_sclk_0032.tsc"
    )
    wiremock_manager.add_file_mapping(
        "/download/imap/spice/sclk/imap_sclk_0032.tsc",
        str(spice_file_path),
    )

    with Environment(
        IMAP_DATA_ACCESS_URL=wiremock_manager.get_url(),
        IMAP_API_KEY="12345",
    ):
        data_access = SDCDataAccess(
            auth_code=SecretStr("12345"),
            data_dir=temp_folder_path,
            sdc_url=wiremock_manager.get_url(),
        )

        # Import and test fetch_spice
        from imap_mag.cli.fetch.spice import fetch_spice

        downloaded = fetch_spice(
            data_access=data_access,
            ingest_start_day=datetime(2025, 11, 1),
        )

    # Verify download
    assert len(downloaded) == 1

    # Check that file was downloaded
    downloaded_file = next(iter(downloaded))
    assert downloaded_file[0].exists()
    assert downloaded_file[0].stat().st_size > 0
    assert downloaded_file[0].name == "imap_sclk_0032.tsc"

    # Check metadata
    metadata = downloaded[0][2]
    assert metadata["file_name"] == "sclk/imap_sclk_0032.tsc"
    assert metadata["kernel_type"] == "spacecraft_clock"
    assert metadata["version"] == 32


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
def test_spice_download_multiple_files(wiremock_manager, temp_folder_path):
    """Test downloading multiple SPICE files."""
    wiremock_manager.reset()

    # Setup query response with all sample data
    sample_data = get_sample_spice_data()

    wiremock_manager.add_string_mapping(
        "/spice-query?start_ingest_date=20251101",
        json.dumps(sample_data),
        priority=1,
    )

    # Setup download responses - we'll use the same file for all downloads as a test
    # Note: imap_data_access adds /imap/spice/ prefix to the download path
    spice_file_path = (
        Path(__file__).parent / "test_data" / "spice" / "imap_sclk_0032.tsc"
    )

    for item in sample_data:
        wiremock_manager.add_file_mapping(
            f"/download/imap/spice/{item['file_name']}",
            str(spice_file_path),
        )

    with Environment(
        IMAP_DATA_ACCESS_URL=wiremock_manager.get_url(),
        IMAP_API_KEY="12345",
    ):
        data_access = SDCDataAccess(
            auth_code=SecretStr("12345"),
            data_dir=temp_folder_path,
            sdc_url=wiremock_manager.get_url(),
        )

        # Import and test fetch_spice
        from imap_mag.cli.fetch.spice import fetch_spice

        downloaded = fetch_spice(
            data_access=data_access,
            ingest_start_day=datetime(2025, 11, 1),
        )

    # Verify all files were downloaded
    assert len(downloaded) == 3

    # Check that all files exist
    for downloaded_file, handler, metadata in downloaded:
        assert downloaded_file.exists()
        assert downloaded_file.stat().st_size > 0
        assert metadata["file_name"] in [item["file_name"] for item in sample_data]
