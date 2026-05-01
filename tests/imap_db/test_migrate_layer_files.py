"""Tests for the 2026_05_01-52c7b098641d_migrate_layer_files Alembic migration."""

import importlib.util
import os
from datetime import datetime
from pathlib import Path

import numpy as np
import pytest

from imap_db.model import File
from imap_mag import __version__
from imap_mag.io.file import IFilePathHandler
from mag_toolkit.calibration import CalibrationLayer
from mag_toolkit.calibration.CalibrationDefinitions import (
    CalibrationMetadata,
    CalibrationMethod,
    Mission,
    Sensor,
    ValueType,
)
from mag_toolkit.calibration.Layer import Validity
from tests.util.database import test_database, test_database_server_engine  # noqa: F401

# ---------------------------------------------------------------------------
# Load the migration module via importlib (filename contains dashes)
# ---------------------------------------------------------------------------
_MIGRATION_PATH = (
    Path(__file__).parent.parent.parent
    / "src/imap_db/migrations/versions/2026_05_01-52c7b098641d_migrate_layer_files.py"
)
_spec = importlib.util.spec_from_file_location("migrate_layer_files", _MIGRATION_PATH)
_migration = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(_migration)  # type: ignore[union-attr]
_run_migration = _migration._run_migration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_old_layer_pair(
    folder: Path, descriptor: str, date: datetime, version: int, csv_content: str
) -> tuple[Path, Path]:
    """Write a JSON+CSV pair WITHOUT data_hash (simulating files from the old code)."""
    from imap_mag.io.file import CalibrationLayerPathHandler

    handler = CalibrationLayerPathHandler(
        descriptor=descriptor, content_date=date, version=version
    )
    csv_name = handler.get_equivalent_data_handler().get_filename()
    csv_path = folder / csv_name
    csv_path.write_text(csv_content)

    # Build the CalibrationLayer without setting _contents so that
    # CalibrationLayer._write_to_json will NOT compute / write data_hash.
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
            # data_hash intentionally omitted
        ),
        value_type=ValueType.BOUNDARY_CHANGES_ONLY,
        method=CalibrationMethod.SET_QUALITY_AND_NAN,
    )
    json_path = folder / handler.get_filename()
    layer.writeToFile(json_path)

    assert json_path.exists()
    assert csv_path.exists()
    assert layer.metadata.data_hash is None, (
        "Helper must produce a file with no data_hash"
    )
    return json_path, csv_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers do not work on Windows GitHub Actions",
)
def test_migration_populates_data_hash_and_updates_db_hash(
    test_database,  # noqa: F811
    test_database_server_engine,  # noqa: F811
    tmp_path,
) -> None:
    """Migration populates data_hash in JSON and corrects the DB hash for old files."""
    date = datetime(2026, 1, 1)
    csv_content = (
        "time,offset_x,offset_y,offset_z,timedelta,quality_flag,quality_bitmask\n"
    )

    store_dir = tmp_path / "calibration" / "layers" / "2026" / "01"
    store_dir.mkdir(parents=True)

    # Write old-style layer pair (no data_hash in JSON)
    json_path, csv_path = _write_old_layer_pair(
        store_dir, "quality-norm", date, 1, csv_content
    )
    expected_csv_hash = IFilePathHandler.default_file_hash(csv_path)

    # Insert a DB record with an incorrect/placeholder hash
    relative_json_path = json_path.relative_to(tmp_path)
    test_database.insert_files(
        [
            File(
                name=json_path.name,
                path=relative_json_path.as_posix(),
                descriptor="imap_mag_quality-norm-layer",
                version=1,
                hash="old_incorrect_hash",
                size=json_path.stat().st_size,
                content_date=date,
                creation_date=datetime(2026, 1, 1, 12, 0, 0),
                last_modified_date=datetime(2026, 1, 1, 12, 0, 0),
                software_version=__version__,
            )
        ]
    )

    # Run the migration
    with test_database_server_engine.begin() as conn:
        _run_migration(conn, tmp_path)

    # JSON file now has data_hash
    updated_layer = CalibrationLayer.from_file(json_path, load_contents=False)
    assert updated_layer.metadata.data_hash == expected_csv_hash

    # DB hash record is corrected
    db_files = test_database.get_files()
    assert len(db_files) == 1
    assert db_files[0].hash == expected_csv_hash


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers do not work on Windows GitHub Actions",
)
def test_migration_skips_file_already_having_data_hash(
    test_database,  # noqa: F811
    test_database_server_engine,  # noqa: F811
    tmp_path,
) -> None:
    """Migration does not rewrite JSON that already carries a data_hash."""

    date = datetime(2026, 1, 2)
    csv_content = (
        "time,offset_x,offset_y,offset_z,timedelta,quality_flag,quality_bitmask\n"
    )

    store_dir = tmp_path / "calibration" / "layers" / "2026" / "01"
    store_dir.mkdir(parents=True)

    # Use the full helper that DOES write data_hash (current code path)
    from tests.test_DBIndexedDatastoreFileManager import _write_layer_pair

    json_path, csv_path = _write_layer_pair(
        store_dir, "quality-norm", date, 1, csv_content
    )
    expected_hash = IFilePathHandler.default_file_hash(csv_path)

    # DB record already has the correct hash
    relative_json_path = json_path.relative_to(tmp_path)
    test_database.insert_files(
        [
            File(
                name=json_path.name,
                path=relative_json_path.as_posix(),
                descriptor="imap_mag_quality-norm-layer",
                version=1,
                hash=expected_hash,
                size=json_path.stat().st_size,
                content_date=date,
                creation_date=datetime(2026, 1, 2, 12, 0, 0),
                last_modified_date=datetime(2026, 1, 2, 12, 0, 0),
                software_version=__version__,
            )
        ]
    )

    json_mtime_before = json_path.stat().st_mtime

    with test_database_server_engine.begin() as conn:
        _run_migration(conn, tmp_path)

    # JSON file must not have been rewritten
    assert json_path.stat().st_mtime == json_mtime_before
    db_files = test_database.get_files()
    assert db_files[0].hash == expected_hash


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers do not work on Windows GitHub Actions",
)
def test_migration_skips_deleted_db_records(
    test_database,  # noqa: F811
    test_database_server_engine,  # noqa: F811
    tmp_path,
) -> None:
    """Migration ignores soft-deleted file records."""
    date = datetime(2026, 1, 3)
    csv_content = (
        "time,offset_x,offset_y,offset_z,timedelta,quality_flag,quality_bitmask\n"
    )

    store_dir = tmp_path / "calibration" / "layers" / "2026" / "01"
    store_dir.mkdir(parents=True)
    json_path, _ = _write_old_layer_pair(
        store_dir, "quality-norm", date, 1, csv_content
    )

    relative_json_path = json_path.relative_to(tmp_path)
    test_database.insert_files(
        [
            File(
                name=json_path.name,
                path=relative_json_path.as_posix(),
                descriptor="imap_mag_quality-norm-layer",
                version=1,
                hash="old_hash",
                size=json_path.stat().st_size,
                content_date=date,
                creation_date=datetime(2026, 1, 3, 12, 0, 0),
                last_modified_date=datetime(2026, 1, 3, 12, 0, 0),
                deletion_date=datetime(2026, 1, 4, 0, 0, 0),  # soft-deleted
                software_version=__version__,
            )
        ]
    )

    with test_database_server_engine.begin() as conn:
        _run_migration(conn, tmp_path)

    # DB hash must remain unchanged for the deleted record
    all_files = test_database.get_files()
    assert all_files[0].hash == "old_hash"

    # JSON must not have been rewritten (no data_hash)
    layer = CalibrationLayer.from_file(json_path, load_contents=False)
    assert layer.metadata.data_hash is None


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers do not work on Windows GitHub Actions",
)
def test_migration_skips_missing_datastore_file_gracefully(
    test_database,  # noqa: F811
    test_database_server_engine,  # noqa: F811
    tmp_path,
) -> None:
    """Migration logs a warning and continues when the JSON file is absent from disk."""
    date = datetime(2026, 1, 4)

    # Insert a DB record that points to a non-existent file
    test_database.insert_files(
        [
            File(
                name="imap_mag_quality-norm-layer_20260104_v001.json",
                path="calibration/layers/2026/01/imap_mag_quality-norm-layer_20260104_v001.json",
                descriptor="imap_mag_quality-norm-layer",
                version=1,
                hash="some_hash",
                size=100,
                content_date=date,
                creation_date=datetime(2026, 1, 4, 12, 0, 0),
                last_modified_date=datetime(2026, 1, 4, 12, 0, 0),
                software_version=__version__,
            )
        ]
    )

    # Must not raise — missing file is a warning, not a failure
    with test_database_server_engine.begin() as conn:
        _run_migration(conn, tmp_path)

    # DB record remains untouched
    db_files = test_database.get_files()
    assert db_files[0].hash == "some_hash"
