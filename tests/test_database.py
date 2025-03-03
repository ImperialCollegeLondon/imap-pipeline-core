"""Tests for database classes."""

import hashlib
import tempfile
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest
import typer

from imap_db.model import File
from imap_mag import __version__
from imap_mag.DB import DatabaseFileOutputManager, IDatabase
from imap_mag.outputManager import IOutputManager, StandardSPDFMetadataProvider

from .testUtils import create_test_file, enableLogging, tidyDataFolders  # noqa: F401


@pytest.fixture
def mock_output_manager() -> mock.Mock:
    """Fixture for a mock IOutputManager instance."""
    return mock.create_autospec(IOutputManager, spec_set=True)


@pytest.fixture
def mock_database() -> mock.Mock:
    """Fixture for a mock IDatabase instance."""
    return mock.create_autospec(IDatabase, spec_set=True)


def test_database_output_manager_writes_to_database(
    mock_output_manager: mock.Mock, mock_database: mock.Mock
) -> None:
    # Set up.
    database_manager = DatabaseFileOutputManager(mock_output_manager, mock_database)

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    metadata_provider = StandardSPDFMetadataProvider(
        version=1, descriptor="hsk-pw", date=datetime(2025, 5, 2), extension="txt"
    )

    test_file = Path(tempfile.gettempdir()) / "test_file.txt"
    mock_output_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        metadata_provider,
    )

    def check_inserted_file(file: File):
        # Two instances of `File` will never be equal, so we check the attributes.
        assert file.name == "test_file.txt"
        assert file.path == test_file.absolute().as_posix()
        assert file.version == 1
        assert file.hash == hashlib.md5(b"some content").hexdigest()
        assert file.date == datetime(2025, 5, 2)
        assert file.software_version == __version__

    mock_database.insert_file.side_effect = lambda file: check_inserted_file(file)

    # Exercise.
    (actual_file, actual_metadata_provider) = database_manager.add_file(
        original_file, metadata_provider
    )

    # Verify.
    mock_output_manager.add_file.assert_called_once_with(
        original_file, metadata_provider
    )

    assert actual_file == test_file
    assert actual_metadata_provider == metadata_provider


def test_database_output_manager_errors_when_destination_file_is_not_found(
    mock_output_manager: mock.Mock, mock_database: mock.Mock
) -> None:
    # Set up.
    database_manager = DatabaseFileOutputManager(mock_output_manager, mock_database)

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    metadata_provider = StandardSPDFMetadataProvider(
        version=1, descriptor="hsk-pw", date=datetime(2025, 5, 2), extension="txt"
    )

    test_file = Path(tempfile.gettempdir()) / "test_file.txt"
    test_file.unlink(missing_ok=True)

    mock_output_manager.add_file.side_effect = lambda *_: (
        test_file,
        metadata_provider,
    )

    # Exercise and verify.
    with pytest.raises(typer.Abort):
        database_manager.add_file(original_file, metadata_provider)


def test_database_output_manager_errors_destination_file_different_hash(
    mock_output_manager: mock.Mock, mock_database: mock.Mock
) -> None:
    # Set up.
    database_manager = DatabaseFileOutputManager(mock_output_manager, mock_database)

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    metadata_provider = StandardSPDFMetadataProvider(
        version=1, descriptor="hsk-pw", date=datetime(2025, 5, 2), extension="txt"
    )

    test_file = Path(tempfile.gettempdir()) / "test_file.txt"
    mock_output_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some other content"),
        metadata_provider,
    )

    # Exercise and verify.
    with pytest.raises(typer.Abort):
        database_manager.add_file(original_file, metadata_provider)


def test_database_output_manager_errors_database_error(
    mock_output_manager: mock.Mock, mock_database: mock.Mock
) -> None:
    # Set up.
    database_manager = DatabaseFileOutputManager(mock_output_manager, mock_database)

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    metadata_provider = StandardSPDFMetadataProvider(
        version=1, descriptor="hsk-pw", date=datetime(2025, 5, 2), extension="txt"
    )

    test_file = Path(tempfile.gettempdir()) / "test_file.txt"
    mock_output_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        metadata_provider,
    )

    mock_database.insert_file.side_effect = ArithmeticError("Database error")

    # Exercise and verify.
    with pytest.raises(ArithmeticError):
        database_manager.add_file(original_file, metadata_provider)
