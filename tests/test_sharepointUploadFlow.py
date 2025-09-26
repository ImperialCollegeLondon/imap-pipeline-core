import tempfile
from datetime import datetime
from pathlib import Path

import pytest
from prefect.filesystems import LocalFileSystem

from imap_db.model import File
from imap_mag.config.AppSettings import AppSettings
from imap_mag.util import Environment
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.sharepointUploadFlow import upload_new_files_to_sharepoint
from tests.util.miscellaneous import DATASTORE


@pytest.mark.asyncio
async def test_upload_new_files_to_sharepoint_does_upload_a_file_locally(
    capture_cli_logs, test_database
):
    # Set up.
    upload_file = Path(
        "tests/datastore/science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )
    sharepoint = Path(tempfile.mkdtemp())
    with Environment(
        MAG_DATA_STORE=str(DATASTORE),
    ):
        test_database.insert_file(
            File.from_file(
                upload_file, 1, "NOT-REAL-HASH", datetime(2025, 10, 17), AppSettings()
            )
        )
        destination = LocalFileSystem(basepath=sharepoint.as_posix())
        await destination.save(PREFECT_CONSTANTS.SHAREPOINT_BLOCK_NAME, overwrite=True)

        expected_path = (
            sharepoint
            / "Flight Data"
            / upload_file.absolute().relative_to(DATASTORE.absolute())
        )
        expected_path.unlink(missing_ok=True)

        # Exercise.
        await upload_new_files_to_sharepoint()

    # Verify.
    assert "1 file(s) uploaded to SharePoint" in capture_cli_logs.text
    assert expected_path.exists(), f"Expected file {expected_path} to exist"

    # Exercise again - do nothing
    with Environment(
        MAG_DATA_STORE=str(DATASTORE),
    ):
        await upload_new_files_to_sharepoint()

    # Verify.
    assert "0 file(s) uploaded to SharePoint" in capture_cli_logs.text
