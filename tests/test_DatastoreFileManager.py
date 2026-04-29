"""Tests for `OutputManager` class."""

import json
import re
from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.io import DatastoreFileManager
from imap_mag.io.file import (
    CalibrationLayerPathHandler,
    HKDecodedPathHandler,
    IFilePathHandler,
)
from tests.util.miscellaneous import (
    create_test_file,
    write_calibration_layer_pair,
)


def test_copy_new_file(capture_cli_logs, temp_folder_path):
    # Set up.
    manager = DatastoreFileManager(temp_folder_path)

    original_file = create_test_file(Path(f"{temp_folder_path}/some_test_file.txt"))

    # Exercise.
    manager.add_file(
        original_file,
        HKDecodedPathHandler(
            descriptor="pwr",
            content_date=datetime(2025, 5, 2),
            extension="txt",
        ),
    )

    # Verify.
    assert (
        f"Copied to {Path(f'{temp_folder_path}/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v001.txt')}."
        in capture_cli_logs.text
    )

    assert Path(
        f"{temp_folder_path}/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v001.txt"
    ).exists()


def test_copy_file_same_content(capture_cli_logs, temp_folder_path):
    # Set up.
    manager = DatastoreFileManager(temp_folder_path)

    original_file = create_test_file(
        Path(f"{temp_folder_path}/test_copy_file_same_content.txt"), "some content"
    )
    existing_file = create_test_file(
        Path(
            f"{temp_folder_path}/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v001.txt"
        ),
        "some content",
    )

    existing_modification_time = existing_file.stat().st_mtime

    # Exercise.
    manager.add_file(
        original_file,
        HKDecodedPathHandler(
            descriptor="pwr",
            content_date=datetime(2025, 5, 2),
            extension="txt",
        ),
    )

    # Verify.
    assert (
        f"File {Path(f'{temp_folder_path}/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v001.txt')} already exists and is the same. Skipping update."
        in capture_cli_logs.text
    )

    assert not Path(
        f"{temp_folder_path}/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v002.txt"
    ).exists()
    assert existing_file.stat().st_mtime == existing_modification_time


def test_copy_file_second_existing_file_with_same_content(
    capture_cli_logs, temp_folder_path
):
    # Set up.
    manager = DatastoreFileManager(temp_folder_path)

    original_file = create_test_file(
        Path(
            f"{temp_folder_path}/test_copy_file_second_existing_file_with_same_content.txt"
        ),
        "some content",
    )
    existing_file = create_test_file(
        Path(
            f"{temp_folder_path}/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v002.txt"
        ),
        "some content",
    )
    create_test_file(
        Path(
            f"{temp_folder_path}/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v001.txt"
        )
    )

    existing_modification_time = existing_file.stat().st_mtime

    # Exercise.
    manager.add_file(
        original_file,
        HKDecodedPathHandler(
            descriptor="pwr",
            content_date=datetime(2025, 5, 2),
            extension="txt",
        ),
    )

    # Verify.
    assert (
        f"File {Path(f'{temp_folder_path}/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v001.txt')} already exists and is different. Increasing version to 2."
        in capture_cli_logs.text
    )
    assert (
        f"File {Path(f'{temp_folder_path}/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v002.txt')} already exists and is the same. Skipping update."
        in capture_cli_logs.text
    )

    assert not Path(
        f"{temp_folder_path}/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v003.txt"
    ).exists()
    assert existing_file.stat().st_mtime == existing_modification_time


def test_copy_file_existing_versions(
    capture_cli_logs,
    temp_folder_path,
):
    # Set up.
    manager = DatastoreFileManager(temp_folder_path)

    original_file = create_test_file(
        Path(f"{temp_folder_path}/test_copy_file_existing_versions.txt"), "some content"
    )

    for version in range(1, 3):
        create_test_file(
            Path(
                f"{temp_folder_path}/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v{version:03}.txt"
            )
        )

    # Exercise.
    manager.add_file(
        original_file,
        HKDecodedPathHandler(
            descriptor="pwr",
            content_date=datetime(2025, 5, 2),
            extension="txt",
        ),
    )

    # Verify.
    for version in range(1, 3):
        assert (
            f"File {Path(f'{temp_folder_path}/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v{version:03}.txt')} already exists and is different. Increasing version to {version + 1}."
            in capture_cli_logs.text
        )

    assert Path(
        f"{temp_folder_path}/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v002.txt"
    ).exists()


def test_copy_file_forced_version(temp_folder_path):
    # Set up.
    manager = DatastoreFileManager(temp_folder_path)

    original_file = create_test_file(
        Path(f"{temp_folder_path}/test_copy_file_forced_version.txt")
    )

    # Exercise.
    manager.add_file(
        original_file,
        HKDecodedPathHandler(
            descriptor="pwr",
            content_date=datetime(2025, 5, 2),
            version=3,
            extension="txt",
        ),
    )

    # Verify.
    assert Path(
        f"{temp_folder_path}/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v003.txt"
    ).exists()


class CustomPathHandler(IFilePathHandler):
    def __init__(self, folder: str, name: str) -> None:
        self.folder = folder
        self.name = name

    def supports_sequencing(self) -> bool:
        return False

    def get_content_date_for_indexing(self) -> datetime | None:
        return None

    def get_folder_structure(self) -> str:
        return self.folder

    def get_filename(self) -> str:
        return self.name

    def add_metadata(self, metadata: dict) -> None:
        raise NotImplementedError()

    def get_metadata(self) -> dict | None:
        return None

    @classmethod
    def from_filename(cls, filename: str | Path) -> "CustomPathHandler | None":
        return None


def test_copy_file_same_origin_destination(temp_folder_path, caplog):
    # Set up.
    manager = DatastoreFileManager(temp_folder_path)

    original_file = create_test_file(
        Path(f"{temp_folder_path}/test_copy_file_same_origin_destination.txt")
    )

    original_mod_time = original_file.stat().st_mtime

    # Exercise.
    (new_file, _) = manager.add_file(
        original_file,
        CustomPathHandler(
            folder=temp_folder_path.as_posix(),
            name="test_copy_file_same_origin_destination.txt",
        ),
    )

    # Verify.
    assert original_file.exists() and new_file.exists()
    assert original_file.samefile(new_file)
    assert original_file.stat().st_mtime == original_mod_time

    assert "Source and destination files are the same" in caplog.text


def test_error_on_file_not_found(capture_cli_logs):
    # Set up.
    manager = DatastoreFileManager(Path("output"))

    original_file = Path("does_not/exist.right?")

    # Exercise and verify.
    with pytest.raises(
        FileNotFoundError, match=re.escape(f"File {original_file!s} does not exist.")
    ):
        manager.add_file(original_file, HKDecodedPathHandler())

    assert f"File {original_file} does not exist." in capture_cli_logs.text


# ── CalibrationLayerPathHandler deduplication (file-based datastore) ──────────


def test_calibration_layer_identical_content_deduplicates_to_existing_version(
    capture_cli_logs, temp_folder_path
):
    """If the companion CSV content matches an existing version, reuse that version."""
    date = datetime(2026, 1, 16)

    # Existing v001 pair already in datastore
    store_dir = temp_folder_path / "calibration" / "layers" / "2026" / "01"
    store_dir.mkdir(parents=True)
    write_calibration_layer_pair(store_dir, "quality-norm", date, 1, seed=0)

    # New run produces a work-folder pair also at v001 with identical CSV
    work_dir = temp_folder_path / "work"
    work_dir.mkdir()
    work_json, _ = write_calibration_layer_pair(
        work_dir, "quality-norm", date, 1, seed=0
    )

    manager = DatastoreFileManager(temp_folder_path)
    handler = CalibrationLayerPathHandler(
        descriptor="quality-norm", content_date=date, version=1
    )

    (result_path, _) = manager.add_file(work_json, handler)

    assert result_path.name == "imap_mag_quality-norm-layer_20260116_v001.json"
    assert handler.version == 1
    assert not (store_dir / "imap_mag_quality-norm-layer_20260116_v002.json").exists()


def test_calibration_layer_different_content_bumps_to_v002(
    capture_cli_logs, temp_folder_path
):
    """If the companion CSV content differs from all existing versions, create v002."""
    date = datetime(2026, 1, 16)

    # Existing v001 pair with original content
    store_dir = temp_folder_path / "calibration" / "layers" / "2026" / "01"
    store_dir.mkdir(parents=True)
    write_calibration_layer_pair(store_dir, "quality-norm", date, 1, seed=0)

    # New run with different CSV content
    work_dir = temp_folder_path / "work"
    work_dir.mkdir()
    work_json, _ = write_calibration_layer_pair(
        work_dir, "quality-norm", date, 1, seed=1
    )

    manager = DatastoreFileManager(temp_folder_path)
    handler = CalibrationLayerPathHandler(
        descriptor="quality-norm", content_date=date, version=1
    )

    (result_path, _) = manager.add_file(work_json, handler)

    assert result_path.name == "imap_mag_quality-norm-layer_20260116_v002.json"
    assert handler.version == 2

    # JSON in datastore at v002 must reference v002 CSV
    saved = json.loads(result_path.read_text())
    assert saved["metadata"]["data_filename"] == (
        "imap_mag_quality-norm-layer-data_20260116_v002.csv"
    )


def test_calibration_layer_csv_saved_at_matching_version(temp_folder_path):
    """The companion CSV is saved at the same version the JSON was bumped to."""
    date = datetime(2026, 1, 16)

    store_dir = temp_folder_path / "calibration" / "layers" / "2026" / "01"
    store_dir.mkdir(parents=True)
    write_calibration_layer_pair(store_dir, "quality-norm", date, 1, seed=0)

    work_dir = temp_folder_path / "work"
    work_dir.mkdir()
    work_json, work_csv = write_calibration_layer_pair(
        work_dir, "quality-norm", date, 1, seed=1
    )

    manager = DatastoreFileManager(temp_folder_path)
    json_handler = CalibrationLayerPathHandler(
        descriptor="quality-norm", content_date=date, version=1
    )
    manager.add_file(work_json, json_handler)

    csv_handler = json_handler.get_equivalent_data_handler()  # version now 2
    (csv_result, _) = manager.add_file(work_csv, csv_handler)

    assert csv_result.name == "imap_mag_quality-norm-layer-data_20260116_v002.csv"
    assert csv_result.read_bytes() == work_csv.read_bytes()
