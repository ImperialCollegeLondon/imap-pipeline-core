"""Tests for database classes."""

import hashlib
import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

from imap_db.model import DownloadProgress, File
from imap_mag import __version__
from imap_mag.db import Database, update_database_with_progress
from imap_mag.io import (
    DatabaseFileOutputManager,
    IOutputManager,
)
from imap_mag.io.file import HKDecodedPathHandler
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (  # noqa: F401
    NOW,
    TODAY,
    YESTERDAY,
    create_test_file,
    tidyDataFolders,
)

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def mock_output_manager() -> mock.Mock:
    """Fixture for a mock IOutputManager instance."""
    return mock.create_autospec(IOutputManager, spec_set=True)


@pytest.fixture
def mock_database() -> mock.Mock:
    """Fixture for a mock Database instance."""
    return mock.create_autospec(Database, spec_set=True)


def check_inserted_file(file: File, test_file: Path, version: int):
    # Two instances of `File` will never be equal, so we check the attributes.
    assert file.name == "test_file.txt"
    assert file.path == test_file.absolute().as_posix()
    assert file.version == version
    assert file.hash == hashlib.md5(b"some content").hexdigest()
    assert file.content_date == datetime(2025, 5, 2)
    assert file.creation_date == datetime.fromtimestamp(test_file.stat().st_ctime)
    assert file.last_modified_date == datetime.fromtimestamp(test_file.stat().st_mtime)
    assert file.deletion_date is None
    assert file.software_version == __version__


def test_database_output_manager_writes_to_database(
    mock_output_manager: mock.Mock, mock_database: mock.Mock
) -> None:
    # Set up.
    database_manager = DatabaseFileOutputManager(mock_output_manager, mock_database)

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    path_handler = HKDecodedPathHandler(
        version=1,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )

    test_file = Path(tempfile.gettempdir()) / "test_file.txt"
    mock_output_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        path_handler,
    )

    mock_database.insert_file.side_effect = lambda file: check_inserted_file(
        file, test_file, version=1
    )

    # Exercise.
    (actual_file, actual_path_handler) = database_manager.add_file(
        original_file, path_handler
    )

    # Verify.
    mock_output_manager.add_file.assert_called_once_with(original_file, path_handler)

    assert actual_file == test_file
    assert actual_path_handler == path_handler


def test_database_output_manager_same_file_already_exists_in_database(
    mock_output_manager: mock.Mock, mock_database: mock.Mock, capture_cli_logs
) -> None:
    # Set up.
    database_manager = DatabaseFileOutputManager(mock_output_manager, mock_database)

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    path_handler = HKDecodedPathHandler(
        version=1,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )

    mock_database.get_files.return_value = [
        File(
            name=path_handler.get_filename(),
            path=path_handler.get_folder_structure(),
            version=1,
            hash=hashlib.md5(b"some content").hexdigest(),
            size=0,
            content_date=datetime(2025, 5, 2),
            software_version=__version__,
        )
    ]

    test_file = Path(tempfile.gettempdir()) / "test_file.txt"
    mock_output_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        path_handler,
    )

    # Exercise.
    (actual_file, actual_path_handler) = database_manager.add_file(
        original_file, path_handler
    )

    # Verify.
    mock_output_manager.add_file.assert_called_once_with(original_file, path_handler)

    mock_database.insert_file.assert_not_called()

    assert (
        f"File {test_file} already exists in database and is the same. Skipping insertion."
        in capture_cli_logs.text
    )

    assert actual_file == test_file
    assert actual_path_handler == path_handler


def test_database_output_manager_same_file_already_exists_as_second_file_in_database(
    mock_output_manager: mock.Mock, mock_database: mock.Mock, capture_cli_logs
) -> None:
    # Set up.
    database_manager = DatabaseFileOutputManager(mock_output_manager, mock_database)

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    path_handler = HKDecodedPathHandler(
        version=1,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )
    matched_path_handler = HKDecodedPathHandler(
        version=2,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )

    mock_database.get_files.side_effect = [
        [
            File(
                name="imap_mag_l1_hsk-pw_20250502_v001.txt",
                path="hk/mag/l1/hsk-pw/2025/05",
                version=1,
                hash="",
                size=0,
                content_date=datetime(2025, 5, 2),
                software_version=__version__,
            ),
            File(
                name="imap_mag_l1_hsk-pw_20250502_v002.txt",
                path="hk/mag/l1/hsk-pw/2025/05",
                version=2,
                hash=hashlib.md5(b"some content").hexdigest(),
                size=0,
                content_date=datetime(2025, 5, 2),
                software_version=__version__,
            ),
        ]
    ]

    test_file = Path(tempfile.gettempdir()) / "test_file.txt"
    mock_output_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        matched_path_handler,
    )

    # Exercise.
    (actual_file, actual_path_handler) = database_manager.add_file(
        original_file, path_handler
    )

    # Verify.
    mock_output_manager.add_file.assert_called_once_with(
        original_file, matched_path_handler
    )

    mock_database.insert_file.assert_not_called()

    assert (
        f"File {test_file} already exists in database and is the same. Skipping insertion."
        in capture_cli_logs.text
    )

    assert actual_file == test_file
    assert actual_path_handler == matched_path_handler


def test_database_output_manager_file_different_hash_already_exists_in_database(
    mock_output_manager: mock.Mock, mock_database: mock.Mock, capture_cli_logs
) -> None:
    # Set up.
    database_manager = DatabaseFileOutputManager(mock_output_manager, mock_database)

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    path_handler = HKDecodedPathHandler(
        version=1,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )
    unique_path_handler = HKDecodedPathHandler(
        version=3,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )

    test_file = Path(tempfile.gettempdir()) / "test_file.txt"
    mock_output_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        unique_path_handler,
    )

    mock_database.get_files.side_effect = [
        [
            File(
                name="imap_mag_l1_hsk-pw_20250502_v001.txt",
                path="hk/mag/l1/hsk-pw/2025/05",
                version=1,
                hash=0,
                size=0,
                content_date=datetime(2025, 5, 2),
                software_version=__version__,
            ),
            File(
                name="imap_mag_l1_hsk-pw_20250502_v002.txt",
                path="hk/mag/l1/hsk-pw/2025/05",
                version=2,
                hash=0,
                size=0,
                content_date=datetime(2025, 5, 2),
                software_version=__version__,
            ),
        ]
    ]
    mock_database.insert_file.side_effect = lambda file: check_inserted_file(
        file, test_file, version=3
    )

    # Exercise.
    (actual_file, actual_path_handler) = database_manager.add_file(
        original_file, path_handler
    )

    # Verify.
    mock_output_manager.add_file.assert_called_once_with(
        original_file, unique_path_handler
    )

    assert (
        f"File {Path('hk/mag/l1/hsk-pw/2025/05/imap_mag_l1_hsk-pw_20250502_v001.txt')} already exists in database and is different. Increasing version to 2."
        in capture_cli_logs.text
    )
    assert (
        f"File {Path('hk/mag/l1/hsk-pw/2025/05/imap_mag_l1_hsk-pw_20250502_v002.txt')} already exists in database and is different. Increasing version to 3."
        in capture_cli_logs.text
    )
    assert f"Inserting {test_file} into database." in capture_cli_logs.text

    assert actual_file == test_file
    assert actual_path_handler == unique_path_handler


def test_database_output_manager_errors_when_destination_file_is_not_found(
    mock_output_manager: mock.Mock, mock_database: mock.Mock
) -> None:
    # Set up.
    database_manager = DatabaseFileOutputManager(mock_output_manager, mock_database)

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    path_handler = HKDecodedPathHandler(
        version=1,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )

    test_file = Path(tempfile.gettempdir()) / "test_file.txt"
    test_file.unlink(missing_ok=True)

    mock_output_manager.add_file.side_effect = lambda *_: (
        test_file,
        path_handler,
    )

    # Exercise and verify.
    with pytest.raises(FileNotFoundError):
        database_manager.add_file(original_file, path_handler)


def test_database_output_manager_errors_destination_file_different_hash(
    mock_output_manager: mock.Mock, mock_database: mock.Mock
) -> None:
    # Set up.
    database_manager = DatabaseFileOutputManager(mock_output_manager, mock_database)

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    path_handler = HKDecodedPathHandler(
        version=1,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )

    test_file = Path(tempfile.gettempdir()) / "test_file.txt"
    mock_output_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some other content"),
        path_handler,
    )

    # Exercise and verify.
    with pytest.raises(FileNotFoundError):
        database_manager.add_file(original_file, path_handler)


def test_database_output_manager_errors_database_error(
    mock_output_manager: mock.Mock, mock_database: mock.Mock
) -> None:
    # Set up.
    database_manager = DatabaseFileOutputManager(mock_output_manager, mock_database)

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    path_handler = HKDecodedPathHandler(
        version=1,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )

    test_file = Path(tempfile.gettempdir()) / "test_file.txt"
    mock_output_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        path_handler,
    )

    mock_database.insert_file.side_effect = ArithmeticError("Database error")

    # Exercise and verify.
    with pytest.raises(ArithmeticError):
        database_manager.add_file(original_file, path_handler)


def test_update_database_no_update_needed_if_latest_timestamp_is_older_than_progress_timestamp(
    capture_cli_logs,
    mock_database,
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"

    assert download_progress.last_checked_date is None
    download_progress.progress_timestamp = TODAY

    mock_database.get_download_progress.return_value = download_progress

    # Exercise
    update_database_with_progress(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        checked_timestamp=NOW,
        latest_timestamp=YESTERDAY,
        logger=LOGGER,
    )

    # Verify
    assert (
        f"Latest downloaded timestamp for packet MAG_SCI_NORM is {YESTERDAY}."
        in capture_cli_logs.text
    )

    assert download_progress.last_checked_date is NOW
    assert download_progress.progress_timestamp is TODAY
    assert mock_database.save.called


def test_update_database_update_needed_no_data(
    capture_cli_logs,
    mock_database,
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"

    assert download_progress.last_checked_date is None
    assert download_progress.progress_timestamp is None

    mock_database.get_download_progress.return_value = download_progress

    # Exercise
    update_database_with_progress(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        checked_timestamp=NOW,
        latest_timestamp=YESTERDAY,
        logger=LOGGER,
    )

    # Verify
    assert (
        f"Latest downloaded timestamp for packet MAG_SCI_NORM is {YESTERDAY}."
        in capture_cli_logs.text
    )

    assert download_progress.last_checked_date is NOW
    assert download_progress.progress_timestamp is YESTERDAY
    assert mock_database.save.called


def test_update_database_update_needed_old_data(
    capture_cli_logs,
    mock_database,
) -> None:
    # Set up
    download_progress = DownloadProgress()
    download_progress.item_name = "MAG_SCI_NORM"

    assert download_progress.last_checked_date is None
    download_progress.progress_timestamp = YESTERDAY

    mock_database.get_download_progress.return_value = download_progress

    # Exercise
    update_database_with_progress(
        packet_name="MAG_SCI_NORM",
        database=mock_database,
        checked_timestamp=NOW,
        latest_timestamp=TODAY,
        logger=LOGGER,
    )

    # Verify
    assert (
        f"Latest downloaded timestamp for packet MAG_SCI_NORM is {TODAY}."
        in capture_cli_logs.text
    )

    assert download_progress.last_checked_date is NOW
    assert download_progress.progress_timestamp is TODAY
    assert mock_database.save.called


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
def test_database_output_manager_real_database(
    mock_output_manager: mock.Mock,
    test_database,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Set up.
    database_manager = DatabaseFileOutputManager(mock_output_manager, test_database)

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    path_handler = HKDecodedPathHandler(
        version=1,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )
    unique_path_handler = HKDecodedPathHandler(
        version=3,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )

    test_file = Path(tempfile.gettempdir()) / "test_file.txt"
    mock_output_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        unique_path_handler,
    )

    test_database.insert_files(
        [
            File(
                name="imap_mag_l1_hsk-pw_20250502_v001.txt",
                path="hk/mag/l1/hsk-pw/2025/05/imap_mag_l1_hsk-pw_20250502_v001.txt",
                version=1,
                hash=0,
                size=123,
                content_date=datetime(2025, 5, 2),
                creation_date=datetime(2025, 5, 2, 12, 34, 56),
                last_modified_date=datetime(2025, 5, 2, 12, 56, 34),
                software_version=__version__,
            ),
            File(
                name="imap_mag_l1_hsk-pw_20250502_v002.txt",
                path="hk/mag/l1/hsk-pw/2025/05/imap_mag_l1_hsk-pw_20250502_v002.txt",
                version=2,
                hash=0,
                size=456,
                content_date=datetime(2025, 5, 2),
                creation_date=datetime(2025, 5, 2, 13, 24, 56),
                last_modified_date=datetime(2025, 5, 2, 13, 56, 24),
                software_version=__version__,
            ),
        ]
    )

    # Exercise.
    (actual_file, actual_path_handler) = database_manager.add_file(
        original_file, path_handler
    )

    # Verify.
    mock_output_manager.add_file.assert_called_once_with(
        original_file, unique_path_handler
    )

    assert (
        f"File {Path('hk/mag/l1/hsk-pw/2025/05/imap_mag_l1_hsk-pw_20250502_v001.txt')} already exists in database and is different. Increasing version to 2."
        in capture_cli_logs.text
    )
    assert (
        f"File {Path('hk/mag/l1/hsk-pw/2025/05/imap_mag_l1_hsk-pw_20250502_v002.txt')} already exists in database and is different. Increasing version to 3."
        in capture_cli_logs.text
    )
    assert f"Inserting {test_file} into database." in capture_cli_logs.text

    assert actual_file == test_file
    assert actual_path_handler == unique_path_handler
