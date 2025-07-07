"""Tests for `SDCDataAccess` class."""

from pathlib import Path

import imap_data_access

from imap_mag.client.SDCDataAccess import SDCDataAccess
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
