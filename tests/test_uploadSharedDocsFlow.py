import tempfile
from datetime import datetime
from pathlib import Path

import pytest
from prefect.filesystems import LocalFileSystem

from imap_db.model import File
from imap_mag.config.AppSettings import AppSettings
from imap_mag.util import Environment
from prefect_server.constants import PREFECT_CONSTANTS
from prefect_server.uploadSharedDocsFlow import upload_shared_docs_flow
from tests.util.miscellaneous import DATASTORE
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401


@pytest.mark.asyncio
async def test_upload_shared_docs_flow_does_upload_a_file_locally(
    capture_cli_logs,
    test_database,
    prefect_test_fixture,  # noqa: F811
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
        await destination.save(
            PREFECT_CONSTANTS.DEFAULT_UPLOAD_DESTINATION_BLOCK_NAME, overwrite=True
        )

        expected_path = (
            sharepoint
            / "Flight Data"
            / upload_file.absolute().relative_to(DATASTORE.absolute())
        )
        expected_path.unlink(missing_ok=True)

        # Exercise.
        await upload_shared_docs_flow()

    # Verify.
    assert "1 file(s) uploaded" in capture_cli_logs.text
    assert expected_path.exists(), f"Expected file {expected_path} to exist"

    # Exercise again - do nothing
    with Environment(
        MAG_DATA_STORE=str(DATASTORE),
    ):
        await upload_shared_docs_flow()

    # Verify.
    assert "0 file(s) uploaded" in capture_cli_logs.text


@pytest.mark.asyncio
async def test_upload_shared_docs_flow_does_delete_a_file_locally(
    capture_cli_logs,
    test_database,
    prefect_test_fixture,  # noqa: F811
):
    # Set up - first upload a file, then mark it as deleted in the DB
    upload_file = Path(
        "tests/datastore/science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )
    sharepoint = Path(tempfile.mkdtemp())

    with Environment(
        MAG_DATA_STORE=str(DATASTORE),
    ):
        # Insert and upload the file first
        file_record = File.from_file(
            upload_file, 1, "NOT-REAL-HASH", datetime(2025, 10, 17), AppSettings()
        )
        test_database.insert_file(file_record)

        destination = LocalFileSystem(basepath=sharepoint.as_posix())
        await destination.save(
            PREFECT_CONSTANTS.DEFAULT_UPLOAD_DESTINATION_BLOCK_NAME, overwrite=True
        )

        expected_path = (
            sharepoint
            / "Flight Data"
            / upload_file.absolute().relative_to(DATASTORE.absolute())
        )

        # Upload first
        await upload_shared_docs_flow(do_deletes=False)
        assert expected_path.exists(), f"Expected file {expected_path} to exist"

        # Now mark the file as deleted in the database
        files = test_database.get_files(name=upload_file.name)
        assert len(files) == 1
        files[0].set_deleted()
        test_database.save(files[0])

        # Exercise - run the delete flow
        await upload_shared_docs_flow(do_uploads=False)

    # Verify the file was deleted
    assert "1 file(s) deleted" in capture_cli_logs.text
    assert not expected_path.exists(), f"Expected file {expected_path} to be deleted"


@pytest.mark.asyncio
async def test_upload_shared_docs_flow_delete_skips_when_no_deleted_files(
    capture_cli_logs,
    test_database,
    prefect_test_fixture,  # noqa: F811
):
    # Set up - no deleted files
    sharepoint = Path(tempfile.mkdtemp())

    with Environment(
        MAG_DATA_STORE=str(DATASTORE),
    ):
        destination = LocalFileSystem(basepath=sharepoint.as_posix())
        await destination.save(
            PREFECT_CONSTANTS.DEFAULT_UPLOAD_DESTINATION_BLOCK_NAME, overwrite=True
        )

        # Exercise - run with deletes only (no files to delete)
        await upload_shared_docs_flow(do_uploads=False)

    # Verify
    assert "0 file(s) deleted" in capture_cli_logs.text


@pytest.mark.asyncio
async def test_upload_shared_docs_flow_delete_respects_patterns(
    capture_cli_logs,
    test_database,
    prefect_test_fixture,  # noqa: F811
):
    # Set up - file that doesn't match upload patterns
    sharepoint = Path(tempfile.mkdtemp())

    with Environment(
        MAG_DATA_STORE=str(DATASTORE),
    ):
        # Create a file record that won't match the upload patterns
        file_record = File(
            name="non_matching_file.txt",
            path="some/other/path/non_matching_file.txt",
            descriptor="non_matching",
            version=1,
            hash="abc123",
            size=100,
            content_date=datetime(2025, 10, 17),
            last_modified_date=datetime(2025, 10, 17),
            deletion_date=datetime.now(),
            software_version="1.0.0",
        )
        test_database.insert_file(file_record)

        destination = LocalFileSystem(basepath=sharepoint.as_posix())
        await destination.save(
            PREFECT_CONSTANTS.DEFAULT_UPLOAD_DESTINATION_BLOCK_NAME, overwrite=True
        )

        # Exercise
        await upload_shared_docs_flow(do_uploads=False)

    # Verify - file should not be deleted because it doesn't match patterns
    assert "0 file(s) deleted" in capture_cli_logs.text


@pytest.mark.asyncio
async def test_upload_shared_docs_flow_tracks_delete_progress_separately(
    capture_cli_logs,
    test_database,
    prefect_test_fixture,  # noqa: F811
):
    # Set up
    upload_file = Path(
        "tests/datastore/science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )
    sharepoint = Path(tempfile.mkdtemp())

    with Environment(
        MAG_DATA_STORE=str(DATASTORE),
    ):
        # Insert the file
        file_record = File.from_file(
            upload_file, 1, "NOT-REAL-HASH", datetime(2025, 10, 17), AppSettings()
        )
        test_database.insert_file(file_record)

        destination = LocalFileSystem(basepath=sharepoint.as_posix())
        await destination.save(
            PREFECT_CONSTANTS.DEFAULT_UPLOAD_DESTINATION_BLOCK_NAME, overwrite=True
        )

        expected_path = (
            sharepoint
            / "Flight Data"
            / upload_file.absolute().relative_to(DATASTORE.absolute())
        )

        # Upload first
        await upload_shared_docs_flow(do_deletes=False)
        assert expected_path.exists()

        # Check progress keys are separate
        upload_progress = test_database.get_workflow_progress("sharepoint-upload")
        delete_progress = test_database.get_workflow_progress(
            "sharepoint-upload-deletes"
        )

        assert upload_progress.progress_timestamp is not None
        # Delete progress should have been checked but no deletes happened
        assert delete_progress.last_checked_date is None

        # Mark file as deleted
        files = test_database.get_files(name=upload_file.name)
        files[0].set_deleted()
        test_database.save(files[0])

        # Run deletes
        await upload_shared_docs_flow(do_uploads=False)

        # Verify delete progress was updated
        delete_progress = test_database.get_workflow_progress(
            "sharepoint-upload-deletes"
        )
        assert delete_progress.progress_timestamp is not None
