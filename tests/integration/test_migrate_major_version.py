"""Tests for the 2026_07_09-d4e5f6a7b8c9_rename_layer_files_to_major_version migration.

This migration (Migration B) renames existing layer files from the legacy _vNNN
naming convention to the new _v001.NNNN major-version scheme, updating both the
on-disk files and the database records.

The test also simulates Migration A (add_version_major_to_files_table) by using
a test database whose schema already includes the version_major column (created via
Base.metadata.create_all).  Migration A's data update only affects science files,
so layer-file records start with version_major=0 (the server default).
"""

import importlib.util
import json
import os
from datetime import datetime
from pathlib import Path

import pytest

from imap_db.model import File
from imap_mag import __version__
from tests.util.database import test_database, test_database_server_engine  # noqa: F401

# ---------------------------------------------------------------------------
# Load Migration B via importlib (filename contains dashes)
# ---------------------------------------------------------------------------
_MIGRATION_B_PATH = (
    Path(__file__).parent.parent.parent
    / "src/imap_db/migrations/versions/2026_07_09-d4e5f6a7b8c9_rename_layer_files_to_major_version.py"
)
_spec_b = importlib.util.spec_from_file_location(
    "rename_layer_files_to_major_version", _MIGRATION_B_PATH
)
_migration_b = importlib.util.module_from_spec(_spec_b)  # type: ignore[arg-type]
_spec_b.loader.exec_module(_migration_b)  # type: ignore[union-attr]
_run_migration_b = _migration_b._run_migration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_legacy_layer_pair(
    folder: Path, descriptor: str, date: datetime, version: int, csv_content: str
) -> tuple[Path, Path]:
    """Write a legacy _vNNN format JSON+CSV pair for migration testing.

    The companion CSV is named by replacing .json with .csv in the JSON filename,
    matching Migration B's lookup logic (``old_name.replace('.json', '.csv')``).

    Args:
        folder: Directory in which to create the files.
        descriptor: Layer descriptor, e.g. ``"quality-norm"``.
        date: Content date of the layer.
        version: Legacy version number (written as _vNNN).
        csv_content: Text content for the companion CSV file.

    Returns:
        Tuple of (json_path, csv_path) for the created files.
    """
    date_str = date.strftime("%Y%m%d")
    json_name = f"imap_mag_{descriptor}-layer_{date_str}_v{version:03d}.json"
    csv_name = f"imap_mag_{descriptor}-layer_{date_str}_v{version:03d}.csv"

    csv_path = folder / csv_name
    csv_path.write_text(csv_content)

    # Minimal JSON with the metadata.data_filename field that Migration B rewrites.
    json_data = {
        "metadata": {
            "data_filename": csv_name,
            "dependencies": [],
            "science": [],
            "creation_timestamp": str(date),
            "content_date": str(date),
        }
    }
    json_path = folder / json_name
    json_path.write_text(json.dumps(json_data, indent=2))

    assert json_path.exists(), f"Helper failed to create JSON file: {json_path}"
    assert csv_path.exists(), f"Helper failed to create CSV file: {csv_path}"
    return json_path, csv_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers do not work on Windows GitHub Actions",
)
def test_migrate_major_version_renames_legacy_layer_files_and_updates_version_major_in_db(
    test_database,  # noqa: F811
    test_database_server_engine,  # noqa: F811
    tmp_path,
) -> None:
    """Migration B renames legacy _v003 layer files to _v001.0003 format, updates
    metadata.data_filename in JSON, and sets version_major=1 in the database.

    Scenario:
        - A legacy JSON (_v003.json) and its companion CSV (_v003.csv) exist on disk.
        - Corresponding DB records carry version_major=0 (pre-Migration-A default).
        - Migration B is applied once (functional) and then again (idempotency check).

    Assertions:
        a) Renamed files _v001.0003.json and _v001.0003.csv exist on disk.
        b) Original _v003 files no longer exist on disk.
        c) JSON metadata.data_filename is updated to the new CSV name.
        d) DB rows carry version_major=1 and the updated name / path.
        e) A second run of Migration B raises no errors and leaves the data unchanged.
    """
    date = datetime(2026, 1, 16)
    initial_last_modified = datetime(2026, 1, 16, 12, 0, 0)
    csv_content = "time,offset_x,offset_y,offset_z\n2026-01-16T00:00:00,1.0,2.0,3.0\n"

    store_dir = tmp_path / "calibration" / "layers" / "2026" / "01"
    store_dir.mkdir(parents=True)

    # Create legacy-format layer pair (_v003 naming).
    json_path, csv_path = _write_legacy_layer_pair(
        store_dir, "quality-norm", date, 3, csv_content
    )

    # Expected names after migration.
    new_json_name = "imap_mag_quality-norm-layer_20260116_v001.0003.json"
    new_csv_name = "imap_mag_quality-norm-layer_20260116_v001.0003.csv"
    new_json_path = store_dir / new_json_name
    new_csv_path = store_dir / new_csv_name

    # Insert DB records simulating the pre-Migration-A state (version_major=0).
    # JSON and CSV must have different descriptors to satisfy the unique constraint
    # (descriptor, content_date, version, deletion_date).
    relative_json = json_path.relative_to(tmp_path).as_posix()
    relative_csv = csv_path.relative_to(tmp_path).as_posix()

    test_database.upsert_files(
        [
            File(
                name=json_path.name,
                path=relative_json,
                descriptor="imap_mag_quality-norm-layer",
                version=3,
                version_major=0,
                hash="placeholder_json_hash",
                size=json_path.stat().st_size,
                content_date=date,
                creation_date=datetime(2026, 1, 16, 12, 0, 0),
                last_modified_date=initial_last_modified,
                software_version="0.0.0",
            ),
            File(
                name=csv_path.name,
                path=relative_csv,
                # Use a distinct descriptor to satisfy the DB unique constraint on
                # (descriptor, content_date, version, deletion_date).
                descriptor="imap_mag_quality-norm-layer-data",
                version=3,
                version_major=0,
                hash="placeholder_csv_hash",
                size=csv_path.stat().st_size,
                content_date=date,
                creation_date=datetime(2026, 1, 16, 12, 0, 0),
                last_modified_date=initial_last_modified,
                software_version="0.0.0",
            ),
        ]
    )

    # Run Migration B (the rename migration).
    with test_database_server_engine.begin() as conn:
        _run_migration_b(conn, tmp_path)

    # --- Assertion (a): renamed files exist on disk ---
    assert new_json_path.exists(), (
        f"Expected renamed JSON {new_json_name!r} to exist after migration."
    )
    assert new_csv_path.exists(), (
        f"Expected renamed CSV {new_csv_name!r} to exist after migration."
    )

    # --- Assertion (b): old files no longer exist ---
    assert not json_path.exists(), (
        f"Legacy JSON {json_path.name!r} should have been removed by migration."
    )
    assert not csv_path.exists(), (
        f"Legacy CSV {csv_path.name!r} should have been removed by migration."
    )

    # --- Assertion (c): JSON metadata.data_filename updated ---
    with open(new_json_path) as fh:
        updated_json = json.load(fh)
    assert updated_json["metadata"]["data_filename"] == new_csv_name, (
        f"Expected metadata.data_filename to be updated to {new_csv_name!r}."
    )

    # --- Assertion (d): DB records have version_major=1 and updated name/path ---
    db_files = test_database.get_files()
    json_records = [f for f in db_files if f.name == new_json_name]
    csv_records = [f for f in db_files if f.name == new_csv_name]

    assert len(json_records) == 1, (
        f"Expected exactly one DB record with name {new_json_name!r}; got {len(json_records)}."
    )
    assert json_records[0].version_major == 1, (
        f"JSON DB record version_major should be 1, got {json_records[0].version_major}."
    )
    assert json_records[0].path == f"calibration/layers/2026/01/{new_json_name}", (
        f"JSON DB record path mismatch: {json_records[0].path!r}."
    )
    assert json_records[0].software_version == __version__, (
        "JSON DB record software_version should be set to the current package version."
    )
    assert json_records[0].last_modified_date > initial_last_modified, (
        "JSON DB record last_modified_date should be updated during migration."
    )

    assert len(csv_records) == 1, (
        f"Expected exactly one DB record with name {new_csv_name!r}; got {len(csv_records)}."
    )
    assert csv_records[0].version_major == 1, (
        f"CSV DB record version_major should be 1, got {csv_records[0].version_major}."
    )
    assert csv_records[0].path == f"calibration/layers/2026/01/{new_csv_name}", (
        f"CSV DB record path mismatch: {csv_records[0].path!r}."
    )
    assert csv_records[0].software_version == __version__, (
        "CSV DB record software_version should be set to the current package version."
    )
    assert csv_records[0].last_modified_date > initial_last_modified, (
        "CSV DB record last_modified_date should be updated during migration."
    )

    # --- Assertion (e): idempotency - second run must not raise ---
    with test_database_server_engine.begin() as conn:
        _run_migration_b(conn, tmp_path)

    # Files and DB records must be unchanged after the second run.
    assert new_json_path.exists(), (
        "JSON file should still exist after idempotent second migration run."
    )
    assert new_csv_path.exists(), (
        "CSV file should still exist after idempotent second migration run."
    )
    db_files_after = test_database.get_files()
    assert len(db_files_after) == 2, (
        "DB record count should be unchanged after idempotent second migration run."
    )
    assert all(f.version_major == 1 for f in db_files_after), (
        "All DB records should still have version_major=1 after second run."
    )
