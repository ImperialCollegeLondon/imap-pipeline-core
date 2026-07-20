"""Tests for the 2026_07_18-f6a7b8c9d0e1_fix_layer_csv_data_filename_and_rename migration.

Migration ``e5f6a7b8c9d0`` (rename_legacy_layer_csv_files) tried to complete
the CSV rename left unfinished by ``d4e5f6a7b8c9``, by deriving the legacy CSV
name from each layer JSON's ``metadata.data_filename``. For many layer files
that field is itself wrong -- it was written as the JSON's own base name with
just the extension swapped (e.g.
``imap_mag_manual-burst-layer_20260501_v001.0001.csv``), omitting the
``-data`` descriptor the real CSV file actually uses (e.g.
``imap_mag_manual-burst-layer-data_20260501_v001.0001.csv``). Because of that,
Migration ``e5f6a7b8c9d0`` derived a legacy name that never matched a real
database record and renamed nothing for these files.

Migration D (this one) corrects the ``-data``-less data_filename in the JSON,
then uses the corrected name to find the legacy CSV DB record, rename the file
on disk, and update its DB record.
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
# Load Migration D via importlib (filename contains dashes)
# ---------------------------------------------------------------------------
_MIGRATION_D_PATH = (
    Path(__file__).parent.parent.parent
    / "src/imap_db/migrations/versions/2026_07_18-f6a7b8c9d0e1_fix_layer_csv_data_filename_and_rename.py"
)
_spec_d = importlib.util.spec_from_file_location(
    "fix_layer_csv_data_filename_and_rename", _MIGRATION_D_PATH
)
_migration_d = importlib.util.module_from_spec(_spec_d)  # type: ignore[arg-type]
_spec_d.loader.exec_module(_migration_d)  # type: ignore[union-attr]
_run_migration_d = _migration_d._run_migration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_layer_pair_with_bad_data_filename(
    folder: Path, descriptor: str, date: datetime, minor: int, csv_content: str
) -> tuple[Path, Path, str, str]:
    """Write files reproducing the state left behind by Migrations B and C.

    The JSON is already renamed to the new _v001.NNNN scheme, but its
    metadata.data_filename is missing the "-data" descriptor (the bug this
    migration fixes). The companion CSV file itself is still on disk under
    its legacy name (``-layer-data_..._vNNN.csv``).

    Args:
        folder: Directory in which to create the files.
        descriptor: Layer descriptor, e.g. ``"manual-burst"``.
        date: Content date of the layer.
        minor: Minor version number (e.g. 1 -> _v001.0001 / legacy _v001).
        csv_content: Text content for the companion CSV file.

    Returns:
        Tuple of (json_path, legacy_csv_path, bad_data_filename, correct_csv_name).
    """
    date_str = date.strftime("%Y%m%d")
    json_name = f"imap_mag_{descriptor}-layer_{date_str}_v001.{minor:04d}.json"
    bad_data_filename = f"imap_mag_{descriptor}-layer_{date_str}_v001.{minor:04d}.csv"
    correct_csv_name = (
        f"imap_mag_{descriptor}-layer-data_{date_str}_v001.{minor:04d}.csv"
    )
    legacy_csv_name = f"imap_mag_{descriptor}-layer-data_{date_str}_v{minor:03d}.csv"

    legacy_csv_path = folder / legacy_csv_name
    legacy_csv_path.write_text(csv_content)

    json_data = {
        "id": json_name,
        "metadata": {
            "data_filename": bad_data_filename,
            "dependencies": [],
            "science": [],
            "creation_timestamp": str(date),
            "content_date": str(date),
        },
    }
    json_path = folder / json_name
    json_path.write_text(json.dumps(json_data, indent=2))

    assert json_path.exists(), f"Helper failed to create JSON file: {json_path}"
    assert legacy_csv_path.exists(), (
        f"Helper failed to create legacy CSV file: {legacy_csv_path}"
    )
    return json_path, legacy_csv_path, bad_data_filename, correct_csv_name


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Test containers do not work on Windows GitHub Actions",
)
def test_migrate_fixes_bad_data_filename_and_renames_legacy_csv(
    test_database,  # noqa: F811
    test_database_server_engine,  # noqa: F811
    tmp_path,
) -> None:
    """Migration D fixes a data_filename missing "-data" and renames the legacy CSV.

    Scenario:
        - The layer JSON has already been renamed to the new _v001.0001 scheme
          (by Migration B), but its metadata.data_filename is missing the
          "-data" descriptor (the bug this migration fixes).
        - The companion CSV file is still on disk (and in the DB) under its
          legacy name, since Migration C's lookup (based on the bad
          data_filename) never matched it.
        - Migration D is applied once (functional) and then again (idempotency
          check).

    Assertions:
        a) The JSON's metadata.data_filename is corrected to include "-data".
        b) The CSV file is renamed on disk to the corrected new-style name.
        c) The legacy CSV file no longer exists on disk.
        d) The CSV DB row is updated to the new name/path with version_major=1.
        e) A second run of Migration D raises no errors and leaves data unchanged.
    """
    date = datetime(2026, 5, 1)
    initial_last_modified = datetime(2026, 5, 1, 12, 0, 0)
    csv_content = "time,offset_x,offset_y,offset_z\n2026-05-01T00:00:00,1.0,2.0,3.0\n"

    store_dir = tmp_path / "calibration" / "layers" / "2026" / "05"
    store_dir.mkdir(parents=True)

    json_path, legacy_csv_path, _bad_data_filename, correct_csv_name = (
        _write_layer_pair_with_bad_data_filename(
            store_dir, "manual-burst", date, 1, csv_content
        )
    )

    new_csv_path = store_dir / correct_csv_name

    relative_json = json_path.relative_to(tmp_path).as_posix()
    relative_legacy_csv = legacy_csv_path.relative_to(tmp_path).as_posix()

    # Insert DB records simulating the state left after Migrations B and C ran:
    # JSON already has version_major=1 and the new name; CSV is untouched
    # (still legacy name/path, version_major=0), since Migration C could not
    # find it due to the bad data_filename.
    test_database.upsert_files(
        [
            File(
                name=json_path.name,
                path=relative_json,
                descriptor="imap_mag_manual-burst-layer",
                version=1,
                version_major=1,
                hash="placeholder_json_hash",
                size=json_path.stat().st_size,
                content_date=date,
                creation_date=datetime(2026, 5, 1, 12, 0, 0),
                last_modified_date=initial_last_modified,
                software_version=__version__,
            ),
            File(
                name=legacy_csv_path.name,
                path=relative_legacy_csv,
                descriptor="imap_mag_manual-burst-layer-data",
                version=1,
                version_major=0,
                hash="placeholder_csv_hash",
                size=legacy_csv_path.stat().st_size,
                content_date=date,
                creation_date=datetime(2026, 5, 1, 12, 0, 0),
                last_modified_date=initial_last_modified,
                software_version="0.0.0",
            ),
        ]
    )

    # Run Migration D (the data_filename fix + legacy CSV rename migration).
    with test_database_server_engine.begin() as conn:
        _run_migration_d(conn, tmp_path)

    # --- Assertion (a): JSON metadata.data_filename corrected ---
    with open(json_path) as fh:
        updated_json = json.load(fh)
    assert updated_json["metadata"]["data_filename"] == correct_csv_name, (
        f"Expected metadata.data_filename to be corrected to {correct_csv_name!r}, "
        f"got {updated_json['metadata']['data_filename']!r}."
    )

    # --- Assertion (b): renamed CSV exists on disk ---
    assert new_csv_path.exists(), (
        f"Expected renamed CSV {correct_csv_name!r} to exist after migration."
    )

    # --- Assertion (c): legacy CSV no longer exists ---
    assert not legacy_csv_path.exists(), (
        f"Legacy CSV {legacy_csv_path.name!r} should have been removed by migration."
    )

    # --- Assertion (d): DB records updated ---
    db_files = test_database.get_files()
    csv_records = [f for f in db_files if f.name == correct_csv_name]

    assert len(csv_records) == 1, (
        f"Expected exactly one DB record with name {correct_csv_name!r}; "
        f"got {len(csv_records)}."
    )
    assert csv_records[0].version_major == 1, (
        f"CSV DB record version_major should be 1, got {csv_records[0].version_major}."
    )
    assert csv_records[0].path == f"calibration/layers/2026/05/{correct_csv_name}", (
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
        _run_migration_d(conn, tmp_path)

    assert new_csv_path.exists(), (
        "CSV file should still exist after idempotent second migration run."
    )
    with open(json_path) as fh:
        json_after_second_run = json.load(fh)
    assert json_after_second_run["metadata"]["data_filename"] == correct_csv_name, (
        "metadata.data_filename should be unchanged after idempotent second run."
    )
    db_files_after = test_database.get_files()
    assert len(db_files_after) == 2, (
        "DB record count should be unchanged after idempotent second migration run."
    )
    csv_records_after = [f for f in db_files_after if f.name == correct_csv_name]
    assert len(csv_records_after) == 1, (
        "Should still be exactly one CSV DB record after second run."
    )
    assert csv_records_after[0].version_major == 1, (
        "CSV DB record version_major should still be 1 after second run."
    )
