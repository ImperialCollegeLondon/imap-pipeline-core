"""Tests for database classes."""

import hashlib
import os
import tempfile
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from imap_db.model import Base, File, WorkflowProgress
from imap_mag import __version__
from imap_mag.db import Database, update_database_with_progress
from imap_mag.io import (
    IDatastoreFileManager,
)
from tests.util.database import (  # noqa: F401
    test_database,
    test_database_container,
    test_database_server_engine,
)
from tests.util.miscellaneous import (
    NOW,
    TODAY,
    YESTERDAY,
    create_test_file,
)


@pytest.fixture
def mock_datastore_manager() -> mock.Mock:
    """Fixture for a mock IOutputManager instance."""
    return mock.create_autospec(IDatastoreFileManager, spec_set=True)


@pytest.fixture
def mock_database() -> mock.Mock:
    """Fixture for a mock Database instance."""
    return mock.create_autospec(Database, spec_set=True)


def check_inserted_file(
    file: File, test_file: Path, version: int, file_name: str = "test_file.txt"
):
    # Two instances of `File` will never be equal, so we check the attributes.
    assert file.name == file_name
    assert file.path == test_file.absolute().as_posix()
    assert file.version == version
    assert file.hash == hashlib.md5(b"some content").hexdigest()
    assert file.content_date == datetime(2025, 5, 2)
    assert file.creation_date == datetime.fromtimestamp(test_file.stat().st_ctime)
    assert file.last_modified_date == datetime.fromtimestamp(test_file.stat().st_mtime)
    assert file.deletion_date is None
    assert file.software_version == __version__


def test_update_database_no_update_needed_if_latest_timestamp_is_older_than_progress_timestamp(
    capture_cli_logs,
    mock_database,
) -> None:
    # Set up
    workflow_progress = WorkflowProgress()
    workflow_progress.item_name = "MAG_SCI_NORM"

    assert workflow_progress.last_checked_date is None
    workflow_progress.progress_timestamp = TODAY

    mock_database.get_workflow_progress.return_value = workflow_progress

    # Exercise
    update_database_with_progress(
        progress_item_id="MAG_SCI_NORM",
        database=mock_database,
        checked_timestamp=NOW,
        latest_timestamp=YESTERDAY,
    )

    # Verify
    assert (
        f"Latest progress timestamp for MAG_SCI_NORM is {YESTERDAY}."
        in capture_cli_logs.text
    )

    assert workflow_progress.last_checked_date is NOW
    assert workflow_progress.progress_timestamp is TODAY
    assert mock_database.save.called


def test_update_database_update_needed_no_data(
    capture_cli_logs,
    mock_database,
) -> None:
    # Set up
    workflow_progress = WorkflowProgress()
    workflow_progress.item_name = "MAG_SCI_NORM"

    assert workflow_progress.last_checked_date is None
    assert workflow_progress.progress_timestamp is None

    mock_database.get_workflow_progress.return_value = workflow_progress

    # Exercise
    update_database_with_progress(
        progress_item_id="MAG_SCI_NORM",
        database=mock_database,
        checked_timestamp=NOW,
        latest_timestamp=YESTERDAY,
    )

    # Verify
    assert (
        f"Latest progress timestamp for MAG_SCI_NORM is {YESTERDAY}."
        in capture_cli_logs.text
    )

    assert workflow_progress.last_checked_date is NOW
    assert workflow_progress.progress_timestamp is YESTERDAY
    assert mock_database.save.called


def test_update_database_update_needed_old_data(
    capture_cli_logs,
    mock_database,
) -> None:
    # Set up
    workflow_progress = WorkflowProgress()
    workflow_progress.item_name = "MAG_SCI_NORM"

    assert workflow_progress.last_checked_date is None
    workflow_progress.progress_timestamp = YESTERDAY

    mock_database.get_workflow_progress.return_value = workflow_progress

    # Exercise
    update_database_with_progress(
        progress_item_id="MAG_SCI_NORM",
        database=mock_database,
        checked_timestamp=NOW,
        latest_timestamp=TODAY,
    )

    # Verify
    assert (
        f"Latest progress timestamp for MAG_SCI_NORM is {TODAY}."
        in capture_cli_logs.text
    )

    assert workflow_progress.last_checked_date is NOW
    assert workflow_progress.progress_timestamp is TODAY
    assert mock_database.save.called


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
def test_database_insert_file_same_name_different_hash(
    test_database,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Set up.
    test_file1 = create_test_file(
        Path(tempfile.gettempdir()) / "test_file.txt", "some content"
    )
    file1 = File(
        name="test_file.txt",
        path=test_file1.absolute().as_posix(),
        descriptor=File.get_descriptor_from_filename("test_file.txt"),
        version=1,
        hash=hashlib.md5(b"some content").hexdigest(),
        size=0,
        content_date=datetime(2025, 5, 2),
        creation_date=datetime.fromtimestamp(test_file1.stat().st_ctime),
        last_modified_date=datetime.fromtimestamp(test_file1.stat().st_mtime),
        software_version=__version__,
    )

    test_database.insert_file(file1)

    test_file2 = create_test_file(
        Path(tempfile.gettempdir()) / "test_file.txt", "some other content"
    )
    file2 = File(
        name="test_file.txt",
        path=test_file2.absolute().as_posix(),
        descriptor=File.get_descriptor_from_filename("test_file.txt"),
        version=1,
        hash=hashlib.md5(b"some other content").hexdigest(),
        size=0,
        content_date=datetime(2025, 5, 2),
        creation_date=datetime.fromtimestamp(test_file2.stat().st_ctime),
        last_modified_date=datetime.fromtimestamp(test_file2.stat().st_mtime),
        software_version=__version__,
    )

    # Exercise.
    test_database.insert_file(file2)

    # Verify.
    database_files = test_database.get_files(name="test_file.txt")
    assert len(database_files) == 1
    assert database_files[0].hash == hashlib.md5(b"some other content").hexdigest()

    assert (
        f"File {test_file2!s} already exists in database with different hash. Replacing."
        in capture_cli_logs.text
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
def test_multiple_database_instances_share_engine(
    test_database_server_engine,  # noqa: F811
) -> None:
    """Multiple Database instances with the same URL must share one SQLAlchemy engine.

    Before the fix each Database() call created a fresh engine with its own
    connection pool, so N instances consumed N*pool_size connections and could
    exhaust the server's max_connections limit.
    """
    db1 = Database(db_url=test_database_server_engine.url)
    db2 = Database(db_url=test_database_server_engine.url)
    db3 = Database(db_url=test_database_server_engine.url)

    assert db1.engine is db2.engine, (
        "Database instances with the same URL should reuse one cached engine"
    )
    assert db2.engine is db3.engine, (
        "Database instances with the same URL should reuse one cached engine"
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
def test_database_read_methods_return_connections_to_pool(
    test_database_server_engine,  # noqa: F811
) -> None:
    """Read methods must close their sessions so connections return to the pool.

    Before the fix the bare self.session().execute(...) pattern did not
    guarantee session closure, so connections could remain checked-out and
    prevent other callers from obtaining one.
    """
    # Use a pool with a single connection and no overflow so any leak is immediately visible.
    constrained_engine = create_engine(
        test_database_server_engine.url,
        pool_size=1,
        max_overflow=0,
        pool_timeout=1,
    )
    Base.metadata.create_all(constrained_engine)

    db = Database.__new__(Database)
    db.engine = constrained_engine
    db.session = sessionmaker(bind=constrained_engine)

    since = datetime(2020, 1, 1)

    # Calling the same read methods repeatedly must not exhaust the single pooled connection.
    for _ in range(5):
        db.get_files_since(since)
        db.get_all_active_files()
        db.get_all_workflow_progress()
        db.get_files_by_path_pattern("%")
        db.get_active_files_matching_patterns(["*"])

    assert constrained_engine.pool.checkedout() == 0, (
        "All connections should be returned to pool after read methods complete"
    )

    constrained_engine.dispose()
