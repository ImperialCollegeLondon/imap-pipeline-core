"""Tests for `SDCDataAccess` class."""

from pathlib import Path

import imap_data_access
from pydantic import SecretStr

from imap_mag.client.SDCDataAccess import SDCDataAccess
from tests.util.miscellaneous import tidyDataFolders  # noqa: F401


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
