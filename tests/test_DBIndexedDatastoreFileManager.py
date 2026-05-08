"""Tests for database classes."""

import hashlib
import json
import logging
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import numpy as np
import pytest

from imap_db.model import File
from imap_mag import __version__
from imap_mag.db import Database
from imap_mag.io import (
    DatastoreFileManager,
    DBIndexedDatastoreFileManager,
    IDatastoreFileManager,
)
from imap_mag.io.file import (
    AncillaryPathHandler,
    CalibrationLayerPathHandler,
    HKBinaryPathHandler,
    HKDecodedPathHandler,
)
from mag_toolkit.calibration import CalibrationLayer
from mag_toolkit.calibration.CalibrationDefinitions import (
    CalibrationMetadata,
    CalibrationMethod,
    Mission,
    Sensor,
    ValueType,
)
from mag_toolkit.calibration.Layer import Validity
from tests.util.database import test_database  # noqa: F401  # noqa: F401
from tests.util.miscellaneous import (
    create_test_file,
)

LOGGER = logging.getLogger(__name__)


@pytest.fixture
def mock_datastore_manager() -> mock.Mock:
    """Fixture for a mock IDatastoreFileManager instance."""
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
    assert file.deletion_date is None
    assert file.software_version == __version__


def test_DBIndexedDatastoreFileManager_writes_to_database(
    mock_datastore_manager: mock.Mock,
    mock_database: mock.Mock,
) -> None:
    # Set up.
    database_manager = DBIndexedDatastoreFileManager(
        mock_datastore_manager, mock_database
    )

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    path_handler = HKDecodedPathHandler(
        version=1,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )

    test_file = Path(tempfile.gettempdir()) / "test_file1.txt"
    mock_datastore_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        path_handler,
    )

    mock_database.upsert_file.side_effect = lambda file: check_inserted_file(
        file, test_file, version=1, file_name="test_file1.txt"
    )

    # Exercise.
    (actual_file, actual_path_handler) = database_manager.add_file(
        original_file, path_handler
    )

    # Verify.
    mock_datastore_manager.add_file.assert_called_once_with(original_file, path_handler)

    assert actual_file == test_file
    assert actual_path_handler == path_handler


def test_DBIndexedDatastoreFileManager_same_file_already_exists_in_database(
    mock_datastore_manager: mock.Mock,
    mock_database: mock.Mock,
    capture_cli_logs,
    temp_datastore,
    dynamic_work_folder,
) -> None:
    # Set up.
    database_manager = DBIndexedDatastoreFileManager(
        mock_datastore_manager, mock_database
    )

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    path_handler = HKDecodedPathHandler(
        version=1,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )

    existing_file = File(
        name=path_handler.get_filename(),
        path=str(path_handler.get_full_path()),
        descriptor=File.get_descriptor_from_filename(path_handler.get_filename()),
        version=1,
        hash=hashlib.md5(b"some content").hexdigest(),
        size=0,
        content_date=datetime(2025, 5, 2),
        software_version=__version__,
    )

    mock_database.get_files.return_value = [existing_file]
    mock_database.get_files_by_path.return_value = [existing_file]

    test_file = Path(tempfile.gettempdir()) / "test_file.txt"
    mock_datastore_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        path_handler,
    )

    # Exercise.
    (actual_file, actual_path_handler) = database_manager.add_file(
        original_file, path_handler
    )

    # Verify.
    mock_datastore_manager.add_file.assert_called_once_with(original_file, path_handler)

    mock_database.upsert_file.assert_not_called()

    assert (
        f"File {test_file} already exists in database with same hash"
        in capture_cli_logs.text
    )

    assert actual_file == test_file
    assert actual_path_handler == path_handler


def test_DBIndexedDatastoreFileManager_same_file_already_exists_as_second_file_in_database(
    mock_datastore_manager: mock.Mock, mock_database: mock.Mock, capture_cli_logs
) -> None:
    # Set up.
    database_manager = DBIndexedDatastoreFileManager(
        mock_datastore_manager, mock_database
    )

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

    existing_file = File(
        name="imap_mag_l1_hsk-pw_20250502_v002.txt",
        path="hk/mag/l1/hsk-pw/2025/05/imap_mag_l1_hsk-pw_20250502_v002.txt",
        descriptor="imap_mag_l1_hsk-pw",
        version=2,
        hash=hashlib.md5(b"some content").hexdigest(),
        size=0,
        content_date=datetime(2025, 5, 2),
        software_version=__version__,
    )
    mock_database.get_files.side_effect = [
        [
            File(
                name="imap_mag_l1_hsk-pw_20250502_v001.txt",
                path="hk/mag/l1/hsk-pw/2025/05/imap_mag_l1_hsk-pw_20250502_v001.txt",
                descriptor="imap_mag_l1_hsk-pw",
                version=1,
                hash="",
                size=0,
                content_date=datetime(2025, 5, 2),
                software_version=__version__,
            ),
            existing_file,
        ]
    ]
    mock_database.get_files_by_path.return_value = [existing_file]

    test_file = Path(tempfile.gettempdir()) / "test_file.txt"
    mock_datastore_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        matched_path_handler,
    )

    # Exercise.
    (actual_file, actual_path_handler) = database_manager.add_file(
        original_file, path_handler
    )

    # Verify.
    mock_datastore_manager.add_file.assert_called_once_with(
        original_file, matched_path_handler
    )

    mock_database.upsert_file.assert_not_called()

    assert (
        f"File {test_file} already exists in database with same hash"
        in capture_cli_logs.text
    )

    assert actual_file == test_file
    assert actual_path_handler == matched_path_handler


def test_DBIndexedDatastoreFileManager_file_different_hash_already_exists_in_database(
    mock_datastore_manager: mock.Mock, mock_database: mock.Mock, capture_cli_logs
) -> None:
    # Set up.
    database_manager = DBIndexedDatastoreFileManager(
        mock_datastore_manager, mock_database
    )

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

    test_file = Path(tempfile.gettempdir()) / "test_file3.txt"
    mock_datastore_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        unique_path_handler,
    )

    mock_database.get_files.side_effect = [
        [
            File(
                name="imap_mag_l1_hsk-pw_20250502_v001.txt",
                path="hk/mag/l1/hsk-pw/2025/05/imap_mag_l1_hsk-pw_20250502_v001.txt",
                descriptor="imap_mag_l1_hsk-pw",
                version=1,
                hash=0,
                size=0,
                content_date=datetime(2025, 5, 2),
                software_version=__version__,
            ),
            File(
                name="imap_mag_l1_hsk-pw_20250502_v002.txt",
                path="hk/mag/l1/hsk-pw/2025/05/imap_mag_l1_hsk-pw_20250502_v002.txt",
                descriptor="imap_mag_l1_hsk-pw",
                version=2,
                hash=0,
                size=0,
                content_date=datetime(2025, 5, 2),
                software_version=__version__,
            ),
        ]
    ]
    mock_database.upsert_file.side_effect = lambda file: check_inserted_file(
        file, test_file, version=3, file_name="test_file3.txt"
    )

    # Exercise.
    (actual_file, actual_path_handler) = database_manager.add_file(
        original_file, path_handler
    )

    # Verify.
    mock_datastore_manager.add_file.assert_called_once_with(
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
    assert f"Upserting {test_file} into database." in capture_cli_logs.text

    assert actual_file == test_file
    assert actual_path_handler == unique_path_handler


def test_DBIndexedDatastoreFileManager_errors_when_destination_file_is_not_found(
    mock_datastore_manager: mock.Mock, mock_database: mock.Mock
) -> None:
    # Set up.
    database_manager = DBIndexedDatastoreFileManager(
        mock_datastore_manager, mock_database
    )

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

    mock_datastore_manager.add_file.side_effect = lambda *_: (
        test_file,
        path_handler,
    )

    # Exercise and verify.
    with pytest.raises(FileNotFoundError):
        database_manager.add_file(original_file, path_handler)


def test_DBIndexedDatastoreFileManager_errors_database_error(
    mock_datastore_manager: mock.Mock, mock_database: mock.Mock
) -> None:
    # Set up.
    database_manager = DBIndexedDatastoreFileManager(
        mock_datastore_manager, mock_database
    )

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
    mock_datastore_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        path_handler,
    )

    mock_database.upsert_file.side_effect = ArithmeticError("Database error")

    # Exercise and verify.
    with pytest.raises(ArithmeticError):
        database_manager.add_file(original_file, path_handler)


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
def test_DBIndexedDatastoreFileManager_real_database_l0_hk_partitioned_file(
    mock_datastore_manager: mock.Mock,
    test_database,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Set up.
    database_manager = DBIndexedDatastoreFileManager(
        mock_datastore_manager, test_database
    )

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    path_handler = HKBinaryPathHandler(
        part=1,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )
    unique_path_handler = HKBinaryPathHandler(
        part=3,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )

    test_file = Path(tempfile.gettempdir()) / "test_file.txt"
    mock_datastore_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        unique_path_handler,
    )

    test_database.upsert_files(
        [
            File(
                name="imap_mag_l0_hsk-pw_20250502_001.txt",
                path="hk/mag/l0/hsk-pw/2025/05/imap_mag_l0_hsk-pw_20250502_001.txt",
                descriptor="imap_mag_l0_hsk-pw",
                version=1,
                hash=0,
                size=123,
                content_date=datetime(2025, 5, 2),
                creation_date=datetime(2025, 5, 2, 12, 34, 56),
                last_modified_date=datetime(2025, 5, 2, 12, 56, 34),
                software_version=__version__,
            ),
            File(
                name="imap_mag_l0_hsk-pw_20250502_002.txt",
                path="hk/mag/l0/hsk-pw/2025/05/imap_mag_l0_hsk-pw_20250502_002.txt",
                descriptor="imap_mag_l0_hsk-pw",
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
    mock_datastore_manager.add_file.assert_called_once_with(
        original_file, unique_path_handler
    )

    assert (
        f"File {Path('hk/mag/l0/hsk-pw/2025/05/imap_mag_l0_hsk-pw_20250502_001.txt')} already exists in database and is different. Increasing version to 2."
        in capture_cli_logs.text
    )
    assert (
        f"File {Path('hk/mag/l0/hsk-pw/2025/05/imap_mag_l0_hsk-pw_20250502_002.txt')} already exists in database and is different. Increasing version to 3."
        in capture_cli_logs.text
    )
    assert f"Upserting {test_file} into database." in capture_cli_logs.text

    assert actual_file == test_file
    assert actual_path_handler == unique_path_handler


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
def test_DBIndexedDatastoreFileManager_real_database_l1_hk_versioned_file(
    mock_datastore_manager: mock.Mock,
    test_database,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Set up.
    database_manager = DBIndexedDatastoreFileManager(
        mock_datastore_manager, test_database
    )

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
    mock_datastore_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        unique_path_handler,
    )

    test_database.upsert_files(
        [
            File(
                name="imap_mag_l1_hsk-pw_20250502_v001.txt",
                path="hk/mag/l1/hsk-pw/2025/05/imap_mag_l1_hsk-pw_20250502_v001.txt",
                descriptor="imap_mag_l1_hsk-pw",
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
                descriptor="imap_mag_l1_hsk-pw",
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
    mock_datastore_manager.add_file.assert_called_once_with(
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
    assert f"Upserting {test_file} into database." in capture_cli_logs.text

    assert actual_file == test_file
    assert actual_path_handler == unique_path_handler


@pytest.mark.parametrize(
    "ancillary_file_name,expected_date",
    [
        ("imap_mag_l2-norm-offsets_20251017_20251017_v001.cdf", datetime(2025, 10, 17)),
        ("imap_mag_l2-calibration_20251017_v001.cdf", None),
        ("imap_mag_l2-calibration_20251017_20251021_v001.cdf", None),
    ],
)
def test_DBIndexedDatastoreFileManager_add_ancillary_files_uses_correct_dates(
    mock_datastore_manager: mock.Mock,
    test_database,  # noqa: F811
    capture_cli_logs,
    ancillary_file_name: str,
    expected_date: datetime,
) -> None:
    # Set Up
    database_manager = DBIndexedDatastoreFileManager(
        mock_datastore_manager, test_database
    )

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / ancillary_file_name, "some content"
    )
    path_handler = AncillaryPathHandler.from_filename(ancillary_file_name)

    assert path_handler is not None

    test_file = Path(tempfile.gettempdir()) / ancillary_file_name
    unique_path_handler = AncillaryPathHandler.from_filename(ancillary_file_name)

    mock_datastore_manager.add_file.side_effect = lambda *_: (
        create_test_file(test_file, "some content"),
        unique_path_handler,
    )

    # Exercise

    database_manager.add_file(original_file, path_handler)

    database_files = test_database.get_files()

    # Verify

    assert f"Upserting {original_file} into database." in capture_cli_logs.text

    assert len(database_files) == 1
    assert database_files[0].name == ancillary_file_name
    assert database_files[0].version == 1
    assert database_files[0].content_date == expected_date


def _write_layer_pair(
    folder: Path, descriptor: str, date: datetime, version: int, csv_content: str
) -> tuple[Path, Path]:
    """Write a v{version} JSON+CSV calibration-layer pair and return their paths."""

    handler = CalibrationLayerPathHandler(
        descriptor=descriptor, content_date=date, version=version
    )

    csv_name = handler.get_equivalent_data_handler().get_filename()
    csv_path = folder / csv_name
    csv_path.write_text(csv_content)

    layer = CalibrationLayer(
        id="",
        mission=Mission.IMAP,
        validity=Validity(start=np.datetime64("NaT"), end=np.datetime64("NaT")),
        sensor=Sensor.MAGO,
        version=handler.version,
        metadata=CalibrationMetadata(
            dependencies=[],
            science=[],
            creation_timestamp=np.datetime64("now"),
            data_filename=Path(csv_name),
            content_date=np.datetime64(date),
        ),
        value_type=ValueType.BOUNDARY_CHANGES_ONLY,
        method=CalibrationMethod.SET_QUALITY_AND_NAN,
    )
    layer._contents = layer._values_from_csv(
        csv_path
    )  # directly set contents without reading from file

    json_path = folder / handler.get_filename()
    layer.writeToFile(json_path)

    assert json_path.exists()
    assert csv_path.exists()

    return json_path, csv_path


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
def test_calibration_layer_db_dedup_identical_content_reuses_v001(
    mock_datastore_manager: mock.Mock,
    test_database,  # noqa: F811
    capture_cli_logs,
    temp_folder_path,
) -> None:
    """Identical companion CSV → version stays at 1, no DB insertion."""
    database_manager = DBIndexedDatastoreFileManager(
        mock_datastore_manager, test_database
    )

    date = datetime(2026, 1, 16)
    csv_content = (
        "time,offset_x,offset_y,offset_z,timedelta,quality_flag,quality_bitmask"
    )

    # Write the layer pair first so we can get the actual CSV hash after the
    # pandas round-trip (which may normalise newlines etc.)
    work_json, work_csv = _write_layer_pair(
        temp_folder_path, "quality-norm", date, 1, csv_content
    )
    csv_hash = hashlib.md5(work_csv.read_bytes()).hexdigest()

    # Pre-populate DB with v001 JSON record whose hash is the companion CSV hash
    test_database.upsert_files(
        [
            File(
                name="imap_mag_quality-norm-layer_20260116_v001.json",
                path="calibration/layers/2026/01/imap_mag_quality-norm-layer_20260116_v001.json",
                descriptor="imap_mag_quality-norm-layer",
                version=1,
                hash=csv_hash,
                size=100,
                content_date=date,
                creation_date=datetime(2026, 1, 16, 12, 0, 0),
                last_modified_date=datetime(2026, 1, 16, 12, 0, 0),
                software_version=__version__,
            )
        ]
    )
    dest_json = (
        Path(tempfile.gettempdir()) / "imap_mag_quality-norm-layer_20260116_v001.json"
    )

    path_handler = CalibrationLayerPathHandler(
        descriptor="quality-norm", content_date=date, version=1
    )
    mock_datastore_manager.add_file.side_effect = lambda *_: (
        create_test_file(
            dest_json,
            json.dumps(
                {
                    "metadata": {
                        "data_filename": "imap_mag_quality-norm-layer-data_20260116_v001.csv"
                    }
                }
            ),
        ),
        path_handler,
    )

    # Exercise
    (_, _) = database_manager.add_file(work_json, path_handler)

    # Verify: deduplication happened — no new DB record
    assert path_handler.version == 1
    db_files = test_database.get_files()
    assert len(db_files) == 1  # only the pre-existing v001 record


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
def test_calibration_layer_db_different_content_creates_v002_with_correct_meta(
    mock_datastore_manager: mock.Mock,
    test_database,  # noqa: F811
    capture_cli_logs,
    temp_folder_path,
) -> None:
    """Different companion CSV → new v002 DB record with updated hash and data_filename."""
    date = datetime(2026, 1, 16)
    old_csv = "time,offset_x,offset_y,offset_z,timedelta,quality_flag,quality_bitmask\n2026-01-16T00:00:00.000000,1.0,2.0,3.0,0.0,0,0\n"
    new_csv = "time,offset_x,offset_y,offset_z,timedelta,quality_flag,quality_bitmask\n2026-01-16T00:00:00.000000,4.0,5.0,6.0,0.0,0,0\n"
    old_csv_hash = hashlib.md5(old_csv.encode()).hexdigest()

    # Write the new layer pair first so we can get the actual CSV hash after the
    # pandas round-trip (which may normalise dates/floats)
    work_json, work_csv = _write_layer_pair(
        temp_folder_path, "quality-norm", date, 1, new_csv
    )
    new_csv_hash = hashlib.md5(work_csv.read_bytes()).hexdigest()

    assert old_csv_hash != new_csv_hash, (
        "old and new CSV hashes must differ for this test"
    )

    test_database.upsert_files(
        [
            File(
                name="imap_mag_quality-norm-layer_20260116_v001.json",
                path="calibration/layers/2026/01/imap_mag_quality-norm-layer_20260116_v001.json",
                descriptor="imap_mag_quality-norm-layer",
                version=1,
                hash=old_csv_hash,
                size=100,
                content_date=date,
                creation_date=datetime(2026, 1, 16, 12, 0, 0),
                last_modified_date=datetime(2026, 1, 16, 12, 0, 0),
                software_version=__version__,
            )
        ]
    )

    path_handler = CalibrationLayerPathHandler(
        descriptor="quality-norm", content_date=date, version=1
    )

    # Capture source content during the mock call (before cleanup deletes the temp file)
    captured_contents: list[dict] = []

    def capture_add_file(
        source: Path, handler
    ) -> tuple[Path, CalibrationLayerPathHandler]:
        captured_contents.append(json.loads(source.read_text()))
        dest = Path(tempfile.gettempdir()) / f"layer_v{handler.version:03d}.json"
        dest.write_bytes(source.read_bytes())
        return dest, handler

    mock_datastore_manager.add_file.side_effect = capture_add_file

    database_manager = DBIndexedDatastoreFileManager(
        mock_datastore_manager, test_database
    )
    database_manager.add_file(work_json, path_handler)

    # Verify version bumped
    assert path_handler.version == 2

    # Verify the source passed to inner manager was the rewritten JSON referencing v002 CSV
    assert captured_contents[0]["metadata"]["data_filename"] == (
        "imap_mag_quality-norm-layer-data_20260116_v002.csv"
    )

    # Verify DB record for v002 stores the companion CSV hash
    db_files = test_database.get_files()
    v002_records = [f for f in db_files if f.version == 2]
    assert len(v002_records) == 1
    assert v002_records[0].hash == new_csv_hash


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
def test_calibration_layer_db_deleted_version_not_compared_or_blocking(
    mock_datastore_manager: mock.Mock,
    test_database,  # noqa: F811
    capture_cli_logs,
    temp_folder_path,
) -> None:
    """Deleted DB records must not block version slots or match on hash.

    Reproduces the bug from the log where v001 and v002 were deleted in the DB,
    causing a new run with identical content to be assigned v003 instead of v001.
    """
    database_manager = DBIndexedDatastoreFileManager(
        mock_datastore_manager, test_database
    )

    date = datetime(2026, 1, 1)
    csv_content = (
        "time,offset_x,offset_y,offset_z,timedelta,quality_flag,quality_bitmask"
    )

    work_json, work_csv = _write_layer_pair(
        temp_folder_path, "quality-norm", date, 1, csv_content
    )
    csv_hash = hashlib.md5(work_csv.read_bytes()).hexdigest()

    # Pre-populate DB with v001 and v002, both soft-deleted
    test_database.upsert_files(
        [
            File(
                name="imap_mag_quality-norm-layer_20260101_v001.json",
                path="calibration/layers/2026/01/imap_mag_quality-norm-layer_20260101_v001.json",
                descriptor="imap_mag_quality-norm-layer",
                version=1,
                hash=csv_hash,
                size=100,
                content_date=date,
                creation_date=datetime(2026, 1, 1, 12, 0, 0),
                last_modified_date=datetime(2026, 1, 1, 12, 0, 0),
                deletion_date=datetime(2026, 1, 2, 0, 0, 0),
                software_version=__version__,
            ),
            File(
                name="imap_mag_quality-norm-layer_20260101_v002.json",
                path="calibration/layers/2026/01/imap_mag_quality-norm-layer_20260101_v002.json",
                descriptor="imap_mag_quality-norm-layer",
                version=2,
                hash="different_hash_for_v002",
                size=100,
                content_date=date,
                creation_date=datetime(2026, 1, 1, 13, 0, 0),
                last_modified_date=datetime(2026, 1, 1, 13, 0, 0),
                deletion_date=datetime(2026, 1, 2, 0, 0, 0),
                software_version=__version__,
            ),
        ]
    )

    path_handler = CalibrationLayerPathHandler(
        descriptor="quality-norm", content_date=date, version=1
    )
    dest_json = (
        Path(tempfile.gettempdir()) / "imap_mag_quality-norm-layer_20260101_v001.json"
    )
    mock_datastore_manager.add_file.side_effect = lambda *_: (
        create_test_file(
            dest_json,
            json.dumps(
                {
                    "metadata": {
                        "data_filename": "imap_mag_quality-norm-layer-data_20260101_v001.csv"
                    }
                }
            ),
        ),
        path_handler,
    )

    # Exercise
    database_manager.add_file(work_json, path_handler)

    # Verify: deleted records did not block slots or match hashes
    # Version must be 1 (not 3), and a new active DB record must be inserted
    assert path_handler.version == 1, (
        f"Expected version 1 but got {path_handler.version}. "
        "Deleted DB records should not occupy version slots."
    )
    active_files = [f for f in test_database.get_files() if f.deletion_date is None]
    assert len(active_files) == 1
    assert active_files[0].version == 1


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
def test_adding_file_to_real_postgres_sets_last_modified_date_in_database(
    mock_datastore_manager: mock.Mock,
    test_database,  # noqa: F811
) -> None:
    # Set up.
    database_manager = DBIndexedDatastoreFileManager(
        mock_datastore_manager, test_database
    )

    original_file = create_test_file(
        Path(tempfile.gettempdir()) / "some_file", "some content"
    )
    path_handler = HKDecodedPathHandler(
        version=1,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )

    test_file = Path(tempfile.gettempdir()) / "test_file_last_modified.txt"
    test_file.unlink(missing_ok=True)

    modified_timestamp = datetime(2025, 5, 3, 12, 34, 56).timestamp()

    def add_file_side_effect(*_) -> tuple[Path, HKDecodedPathHandler]:
        created_file = create_test_file(test_file, "some content")
        os.utime(created_file, (modified_timestamp, modified_timestamp))
        return created_file, path_handler

    mock_datastore_manager.add_file.side_effect = add_file_side_effect

    # Exercise.
    database_manager.add_file(original_file, path_handler)

    database_files = test_database.get_files()
    assert len(database_files) == 1
    # assert last mod is approximately right now
    now = datetime.now(UTC).replace(tzinfo=None)
    assert database_files[0].last_modified_date is not None
    assert abs((database_files[0].last_modified_date - now).total_seconds()) < 5


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
def test_DBIndexedDatastoreFileManager_add_file_with_deleted_file_to_real_postgres_sets_last_modified_date_and_undeletes_in_database(
    test_database,  # noqa: F811
    temp_datastore,
) -> None:
    # Set up.
    real_datastore_manager = DatastoreFileManager(temp_datastore)
    database_manager = DBIndexedDatastoreFileManager(
        real_datastore_manager, test_database
    )

    file_contents = "some content"
    new_file = create_test_file(
        Path(tempfile.gettempdir()) / "imap_mag_l1_hsk-pw_20250502_v001.txt",
        file_contents,
    )
    path_handler = HKDecodedPathHandler(
        version=1,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )
    existing_file_record = File(
        name=path_handler.get_filename(),
        path=str(path_handler.get_full_path()),
        descriptor=File.get_descriptor_from_filename(path_handler.get_filename()),
        version=1,
        hash=hashlib.md5(file_contents.encode()).hexdigest(),
        size=0,
        content_date=datetime(2025, 5, 2),
        software_version=__version__,
        deletion_date=datetime(2025, 5, 3),
        last_modified_date=datetime(2025, 5, 3),
    )

    test_database.upsert_files([existing_file_record])

    # Exercise.
    (actual_file, _) = database_manager.add_file(new_file, path_handler)

    # assert only one file record exists and it is not deleted, with last modified updated to now
    database_files = test_database.get_files()
    assert len(database_files) == 1
    assert "hk/mag/l1/hsk-pw/2025/05/imap_mag_l1_hsk-pw_20250502_v001.txt" in str(
        actual_file
    )
    assert database_files[0].deletion_date is None
    assert database_files[0].last_modified_date is not None
    assert database_files[0].size > 0


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers (used by test database) does not work on Windows",
)
def test_DBIndexedDatastoreFileManager_add_same_file_with_existing_file_to_real_postgres_does_nothing(
    test_database,  # noqa: F811
    temp_datastore,
) -> None:
    # Set up.
    real_datastore_manager = DatastoreFileManager(temp_datastore)
    database_manager = DBIndexedDatastoreFileManager(
        real_datastore_manager, test_database
    )

    file_contents = "some content"
    new_file = create_test_file(
        Path(tempfile.gettempdir()) / "imap_mag_l1_hsk-pw_20250502_v001.txt",
        file_contents,
    )
    path_handler = HKDecodedPathHandler(
        version=1,
        descriptor="hsk-pw",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )
    existing_file_record = File(
        name=path_handler.get_filename(),
        path=str(path_handler.get_full_path()),
        descriptor=File.get_descriptor_from_filename(path_handler.get_filename()),
        version=1,
        hash=hashlib.md5(file_contents.encode()).hexdigest(),
        size=0,
        content_date=datetime(2025, 5, 2),
        last_modified_date=datetime(2025, 5, 3),
        software_version=__version__,
    )

    test_database.upsert_files([existing_file_record])

    # Exercise.
    (actual_file, _) = database_manager.add_file(new_file, path_handler)

    # assert only one file record exists and it is not deleted, with last modified updated to now
    database_files = test_database.get_files()
    assert len(database_files) == 1
    assert "hk/mag/l1/hsk-pw/2025/05/imap_mag_l1_hsk-pw_20250502_v001.txt" in str(
        actual_file
    )
    assert database_files[0].name == "imap_mag_l1_hsk-pw_20250502_v001.txt"
    assert database_files[0].version == 1
    assert database_files[0].hash == hashlib.md5(file_contents.encode()).hexdigest()
    assert database_files[0].path == str(path_handler.get_full_path())
    assert database_files[0].deletion_date is None


class TestDBIndexedDatastoreFileManagerUnit:
    """Unit tests for DBIndexedDatastoreFileManager without Docker."""

    def _make_manager(self, tmp_path, mock_db=None):
        from unittest.mock import MagicMock, patch

        from imap_mag.config.AppSettings import AppSettings
        from imap_mag.io import DBIndexedDatastoreFileManager

        mock_settings = MagicMock(spec=AppSettings)
        mock_settings.data_store = tmp_path

        if mock_db is None:
            mock_db = MagicMock(spec=Database)

        with patch("imap_mag.io.DBIndexedDatastoreFileManager.DatastoreFileManager"):
            manager = DBIndexedDatastoreFileManager(
                database=mock_db,
                settings=mock_settings,
            )

        manager._DBIndexedDatastoreFileManager__settings = mock_settings
        manager._DBIndexedDatastoreFileManager__database = mock_db

        return manager, mock_db, mock_settings

    def test_delete_file_marks_file_as_deleted_and_removes_from_filesystem(
        self, tmp_path
    ):
        from unittest.mock import MagicMock, patch

        from imap_mag.io import DBIndexedDatastoreFileManager

        mock_db = MagicMock(spec=Database)
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path

        source_file = tmp_path / "test_file.csv"
        source_file.write_text("content")

        mock_file = MagicMock(spec=File)
        mock_file.path = "test_file.csv"
        mock_file.deletion_date = None

        with patch("imap_mag.io.DBIndexedDatastoreFileManager.DatastoreFileManager"):
            manager = DBIndexedDatastoreFileManager(
                database=mock_db, settings=mock_settings
            )

        manager._DBIndexedDatastoreFileManager__settings = mock_settings
        manager._DBIndexedDatastoreFileManager__database = mock_db

        manager.delete_file(mock_file)

        mock_file.set_deleted.assert_called_once()
        mock_db.save.assert_called_once_with(mock_file)
        assert not source_file.exists()

    def test_delete_file_does_not_error_when_file_does_not_exist_on_filesystem(
        self, tmp_path
    ):
        from unittest.mock import MagicMock, patch

        from imap_mag.io import DBIndexedDatastoreFileManager

        mock_db = MagicMock(spec=Database)
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path

        mock_file = MagicMock(spec=File)
        mock_file.path = "nonexistent_file.csv"
        mock_file.deletion_date = None

        with patch("imap_mag.io.DBIndexedDatastoreFileManager.DatastoreFileManager"):
            manager = DBIndexedDatastoreFileManager(
                database=mock_db, settings=mock_settings
            )

        manager._DBIndexedDatastoreFileManager__settings = mock_settings
        manager._DBIndexedDatastoreFileManager__database = mock_db

        manager.delete_file(mock_file)

        mock_file.set_deleted.assert_called_once()

    def test_archive_file_copies_to_archive_and_marks_deleted(self, tmp_path):
        from unittest.mock import MagicMock, patch

        from imap_mag.io import DBIndexedDatastoreFileManager

        mock_db = MagicMock(spec=Database)
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path

        source_file = tmp_path / "subdir" / "test_file.csv"
        source_file.parent.mkdir(parents=True)
        source_file.write_text("content to archive")

        archive_folder = tmp_path / "archive"
        archive_folder.mkdir()

        mock_archived_file = MagicMock(spec=File)
        mock_file = MagicMock(spec=File)
        mock_file.path = "subdir/test_file.csv"
        mock_file.archive_to_new_file_path.return_value = mock_archived_file

        with patch("imap_mag.io.DBIndexedDatastoreFileManager.DatastoreFileManager"):
            manager = DBIndexedDatastoreFileManager(
                database=mock_db, settings=mock_settings
            )

        manager._DBIndexedDatastoreFileManager__settings = mock_settings
        manager._DBIndexedDatastoreFileManager__database = mock_db

        manager.archive_file(mock_file, archive_folder)

        assert (archive_folder / "subdir" / "test_file.csv").exists()
        mock_db.insert_file.assert_called_once_with(mock_archived_file)
        mock_db.save.assert_called_once_with(mock_file)
        assert not source_file.exists()
