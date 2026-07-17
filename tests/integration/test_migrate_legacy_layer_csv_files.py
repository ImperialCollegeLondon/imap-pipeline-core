"""Tests for the 2026_07_17-e5f6a7b8c9d0_rename_legacy_layer_csv_files migration.

Migration B (2026_07_09-d4e5f6a7b8c9_rename_layer_files_to_major_version) renamed
layer JSON files to the new _v001.NNNN naming scheme, but had a bug that meant the
companion CSV file (which is not named the same as the JSON, e.g.
``imap_mag_<descriptor>-layer-data_<date>_v<NNN>.csv`` vs.
``imap_mag_<descriptor>-layer_<date>_v<NNN>.json``) was never found, so it was
never renamed on disk or in the database, even though the JSON's
metadata.data_filename was already rewritten to reference the new-style CSV name.

Migration C (this one) re-derives the legacy CSV name from the new-style name
referenced in the already-migrated JSON, finds the CSV's DB record under its
legacy name, renames the file on disk, and updates its DB record.
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
# Load Migration C via importlib (filename contains dashes)
# ---------------------------------------------------------------------------
_MIGRATION_C_PATH = (
    Path(__file__).parent.parent.parent
    / "src/imap_db/migrations/versions/2026_07_17-e5f6a7b8c9d0_rename_legacy_layer_csv_files.py"
)
_spec_c = importlib.util.spec_from_file_location(
    "rename_legacy_layer_csv_files", _MIGRATION_C_PATH
)
_migration_c = importlib.util.module_from_spec(_spec_c)  # type: ignore[arg-type]
_spec_c.loader.exec_module(_migration_c)  # type: ignore[union-attr]
_run_migration_c = _migration_c._run_migration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_post_migration_b_layer_pair(
    folder: Path, descriptor: str, date: datetime, minor: int, csv_content: str
) -> tuple[Path, Path, str]:
    """Write files reproducing the state left behind by buggy Migration B.

    The JSON is already renamed to the new _v001.NNNN scheme, and its
    metadata.data_filename already references the new-style CSV name -- but
    the CSV file itself is still sitting on disk under its legacy name
    (``-layer-data_..._vNNN.csv``), because Migration B's buggy lookup never
    found it.

    Args:
        folder: Directory in which to create the files.
        descriptor: Layer descriptor, e.g. ``"quality-norm"``.
        date: Content date of the layer.
        minor: Minor version number (e.g. 41 -> _v001.0041 / legacy _v041).
        csv_content: Text content for the companion CSV file.

    Returns:
        Tuple of (json_path, legacy_csv_path, new_csv_name).
    """
    date_str = date.strftime("%Y%m%d")
    json_name = f"imap_mag_{descriptor}-layer_{date_str}_v001.{minor:04d}.json"
    new_csv_name = f"imap_mag_{descriptor}-layer-data_{date_str}_v001.{minor:04d}.csv"
    legacy_csv_name = f"imap_mag_{descriptor}-layer-data_{date_str}_v{minor:03d}.csv"

    legacy_csv_path = folder / legacy_csv_name
    legacy_csv_path.write_text(csv_content)

    json_data = {
        "metadata": {
            "data_filename": new_csv_name,
            "dependencies": [],
            "science": [],
            "creation_timestamp": str(date),
            "content_date": str(date),
        }
    }
    json_path = folder / json_name
    json_path.write_text(json.dumps(json_data, indent=2))

    assert json_path.exists(), f"Helper failed to create JSON file: {json_path}"
    assert legacy_csv_path.exists(), (
        f"Helper failed to create legacy CSV file: {legacy_csv_path}"
    )
    return json_path, legacy_csv_path, new_csv_name


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers do not work on Windows GitHub Actions",
)
def test_migrate_legacy_layer_csv_files_renames_csv_and_updates_db(
    test_database,  # noqa: F811
    test_database_server_engine,  # noqa: F811
    tmp_path,
) -> None:
    """Migration C renames a legacy layer-data CSV left behind by buggy Migration B.

    Scenario:
        - The layer JSON has already been renamed to the new _v001.0041 scheme
          (by Migration B) and its metadata.data_filename already points at the
          new-style CSV name.
        - The companion CSV file is still on disk (and in the DB) under its
          legacy name, since Migration B's buggy lookup never matched it.
        - Migration C is applied once (functional) and then again (idempotency
          check).

    Assertions:
        a) The CSV file is renamed on disk to the new-style name.
        b) The legacy CSV file no longer exists on disk.
        c) The JSON file and its metadata are left untouched.
        d) The CSV DB row is updated to the new name/path with version_major=1.
        e) A second run of Migration C raises no errors and leaves data unchanged.
    """
    date = datetime(2026, 4, 28)
    initial_last_modified = datetime(2026, 4, 28, 12, 0, 0)
    csv_content = "time,offset_x,offset_y,offset_z\n2026-04-28T00:00:00,1.0,2.0,3.0\n"

    store_dir = tmp_path / "calibration" / "layers" / "2026" / "04"
    store_dir.mkdir(parents=True)

    json_path, legacy_csv_path, new_csv_name = _write_post_migration_b_layer_pair(
        store_dir, "manual-norm", date, 41, csv_content
    )
    original_json_text = json_path.read_text()

    new_csv_path = store_dir / new_csv_name

    relative_json = json_path.relative_to(tmp_path).as_posix()
    relative_legacy_csv = legacy_csv_path.relative_to(tmp_path).as_posix()

    # Insert DB records simulating the state left after buggy Migration B ran:
    # JSON already has version_major=1 and the new name; CSV is untouched
    # (still legacy name/path, version_major=0).
    test_database.upsert_files(
        [
            File(
                name=json_path.name,
                path=relative_json,
                descriptor="imap_mag_manual-norm-layer",
                version=41,
                version_major=1,
                hash="placeholder_json_hash",
                size=json_path.stat().st_size,
                content_date=date,
                creation_date=datetime(2026, 4, 28, 12, 0, 0),
                last_modified_date=initial_last_modified,
                software_version=__version__,
            ),
            File(
                name=legacy_csv_path.name,
                path=relative_legacy_csv,
                descriptor="imap_mag_manual-norm-layer-data",
                version=41,
                version_major=0,
                hash="placeholder_csv_hash",
                size=legacy_csv_path.stat().st_size,
                content_date=date,
                creation_date=datetime(2026, 4, 28, 12, 0, 0),
                last_modified_date=initial_last_modified,
                software_version="0.0.0",
            ),
        ]
    )

    # Run Migration C (the legacy CSV rename migration).
    with test_database_server_engine.begin() as conn:
        _run_migration_c(conn, tmp_path)

    # --- Assertion (a): renamed CSV exists on disk ---
    assert new_csv_path.exists(), (
        f"Expected renamed CSV {new_csv_name!r} to exist after migration."
    )

    # --- Assertion (b): legacy CSV no longer exists ---
    assert not legacy_csv_path.exists(), (
        f"Legacy CSV {legacy_csv_path.name!r} should have been removed by migration."
    )

    # --- Assertion (c): JSON untouched ---
    assert json_path.exists(), "JSON file should not have been moved or removed."
    assert json_path.read_text() == original_json_text, (
        "JSON file contents should be untouched by Migration C."
    )

    # --- Assertion (d): DB records updated ---
    db_files = test_database.get_files()
    csv_records = [f for f in db_files if f.name == new_csv_name]
    json_records = [f for f in db_files if f.name == json_path.name]

    assert len(csv_records) == 1, (
        f"Expected exactly one DB record with name {new_csv_name!r}; got {len(csv_records)}."
    )
    assert csv_records[0].version_major == 1, (
        f"CSV DB record version_major should be 1, got {csv_records[0].version_major}."
    )
    assert csv_records[0].path == f"calibration/layers/2026/04/{new_csv_name}", (
        f"CSV DB record path mismatch: {csv_records[0].path!r}."
    )
    assert csv_records[0].software_version == __version__, (
        "CSV DB record software_version should be set to the current package version."
    )
    assert csv_records[0].last_modified_date > initial_last_modified, (
        "CSV DB record last_modified_date should be updated during migration."
    )

    assert len(json_records) == 1, "JSON DB record should be unaffected."
    assert json_records[0].last_modified_date == initial_last_modified, (
        "JSON DB record should not be touched by Migration C."
    )

    # --- Assertion (e): idempotency - second run must not raise ---
    with test_database_server_engine.begin() as conn:
        _run_migration_c(conn, tmp_path)

    assert new_csv_path.exists(), (
        "CSV file should still exist after idempotent second migration run."
    )
    db_files_after = test_database.get_files()
    assert len(db_files_after) == 2, (
        "DB record count should be unchanged after idempotent second migration run."
    )
    csv_records_after = [f for f in db_files_after if f.name == new_csv_name]
    assert len(csv_records_after) == 1, (
        "Should still be exactly one CSV DB record after second run."
    )
    assert csv_records_after[0].version_major == 1, (
        "CSV DB record version_major should still be 1 after second run."
    )
