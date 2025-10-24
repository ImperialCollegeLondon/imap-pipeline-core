"""Tests for `OutputManager` class."""

import re
from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.io import OutputManager
from imap_mag.io.file import HKDecodedPathHandler, IFilePathHandler
from tests.util.miscellaneous import (
    create_test_file,
)


def test_copy_new_file(capture_cli_logs, temp_folder_path):
    # Set up.
    manager = OutputManager(temp_folder_path)

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
    manager = OutputManager(temp_folder_path)

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
    manager = OutputManager(temp_folder_path)

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
    manager = OutputManager(temp_folder_path)

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
    manager = OutputManager(temp_folder_path)

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

    @classmethod
    def from_filename(cls, filename: str | Path) -> "CustomPathHandler | None":
        return None


def test_copy_file_same_origin_destination(temp_folder_path, caplog):
    # Set up.
    manager = OutputManager(temp_folder_path)

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
    manager = OutputManager(Path("output"))

    original_file = Path("does_not/exist.right?")

    # Exercise and verify.
    with pytest.raises(
        FileNotFoundError, match=re.escape(f"File {original_file!s} does not exist.")
    ):
        manager.add_file(original_file, HKDecodedPathHandler())

    assert f"File {original_file} does not exist." in capture_cli_logs.text
