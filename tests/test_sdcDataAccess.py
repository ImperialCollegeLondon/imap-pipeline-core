"""Tests for `SDCDataAccess` class."""

import os
from datetime import datetime
from pathlib import Path

import imap_data_access

from imap_mag.client.sdcDataAccess import SDCDataAccess
from tests.util.miscellaneous import tidyDataFolders  # noqa: F401


def test_sdc_data_access_constructor_sets_config() -> None:
    # Set up.
    data_dir = Path("some_test_folder")
    data_access_url = "https://some_test_url"

    # Exercise.
    _ = SDCDataAccess(data_dir, data_access_url)

    # Verify.
    assert imap_data_access.config["DATA_DIR"] == data_dir
    assert imap_data_access.config["DATA_ACCESS_URL"] == data_access_url


def test_get_file_path_builds_file_path() -> None:
    # Set up.
    data_access = SDCDataAccess(Path("some_test_folder"), "some_auth_code")

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
