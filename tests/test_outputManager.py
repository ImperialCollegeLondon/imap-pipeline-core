"""Tests for `OutputManager` class."""

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io import (
    HKPathHandler,
    IFilePathHandler,
    OutputManager,
)
from tests.util.miscellaneous import (  # noqa: F401
    create_test_file,
    tidyDataFolders,
)


def test_copy_new_file(capture_cli_logs):
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = create_test_file(Path(".work/some_test_file.txt"))

    # Exercise.
    manager.add_file(
        original_file,
        HKPathHandler(
            level="l1",
            descriptor="pwr",
            content_date=datetime(2025, 5, 2),
            extension="txt",
        ),
    )

    # Verify.
    assert (
        f"Copied to {Path('output/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v001.txt')}."
        in capture_cli_logs.text
    )

    assert Path(
        "output/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v001.txt"
    ).exists()


def test_copy_file_same_content(capture_cli_logs):
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = create_test_file(Path(".work/some_test_file.txt"), "some content")
    existing_file = create_test_file(
        Path("output/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v001.txt"),
        "some content",
    )

    existing_modification_time = existing_file.stat().st_mtime

    # Exercise.
    manager.add_file(
        original_file,
        HKPathHandler(
            level="l1",
            descriptor="pwr",
            content_date=datetime(2025, 5, 2),
            extension="txt",
        ),
    )

    # Verify.
    assert (
        f"File {Path('output/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v001.txt')} already exists and is the same. Skipping update."
        in capture_cli_logs.text
    )

    assert not Path(
        "output/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v002.txt"
    ).exists()
    assert existing_file.stat().st_mtime == existing_modification_time


def test_copy_file_second_existing_file_with_same_content(capture_cli_logs):
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = create_test_file(Path(".work/some_test_file.txt"), "some content")
    existing_file = create_test_file(
        Path("output/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v002.txt"),
        "some content",
    )
    create_test_file(
        Path("output/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v001.txt")
    )

    existing_modification_time = existing_file.stat().st_mtime

    # Exercise.
    manager.add_file(
        original_file,
        HKPathHandler(
            level="l1",
            descriptor="pwr",
            content_date=datetime(2025, 5, 2),
            extension="txt",
        ),
    )

    # Verify.
    assert (
        f"File {Path('output/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v001.txt')} already exists and is different. Increasing version to 2."
        in capture_cli_logs.text
    )
    assert (
        f"File {Path('output/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v002.txt')} already exists and is the same. Skipping update."
        in capture_cli_logs.text
    )

    assert not Path(
        "output/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v003.txt"
    ).exists()
    assert existing_file.stat().st_mtime == existing_modification_time


def test_copy_file_existing_versions(capture_cli_logs):
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = create_test_file(Path(".work/some_test_file.txt"), "some content")

    for version in range(1, 3):
        create_test_file(
            Path(
                f"output/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v{version:03}.txt"
            )
        )

    # Exercise.
    manager.add_file(
        original_file,
        HKPathHandler(
            level="l1",
            descriptor="pwr",
            content_date=datetime(2025, 5, 2),
            extension="txt",
        ),
    )

    # Verify.
    for version in range(1, 3):
        assert (
            f"File {Path(f'output/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v{version:03}.txt')} already exists and is different. Increasing version to {version + 1}."
            in capture_cli_logs.text
        )

    assert Path(
        "output/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v002.txt"
    ).exists()


def test_copy_file_forced_version():
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = create_test_file(Path(".work/some_test_file.txt"))

    # Exercise.
    manager.add_file(
        original_file,
        HKPathHandler(
            level="l1",
            descriptor="pwr",
            content_date=datetime(2025, 5, 2),
            version=3,
            extension="txt",
        ),
    )

    # Verify.
    assert Path(
        "output/hk/mag/l1/pwr/2025/05/imap_mag_l1_pwr_20250502_v003.txt"
    ).exists()


@dataclass
class TestPathHandler(IFilePathHandler):
    def supports_versioning(self) -> bool:
        return False

    def get_unversioned_pattern(self):
        return re.compile("")

    def get_folder_structure(self) -> str:
        return "abc"

    def get_filename(self) -> str:
        return "def"

    @classmethod
    def from_filename(cls, filename: Path | str) -> "TestPathHandler | None":
        return None


def test_copy_file_custom_providers(capture_cli_logs):
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = create_test_file(Path(".work/some_test_file.txt"))

    # Exercise.
    manager.add_file(original_file, TestPathHandler())

    # Verify.
    assert (
        "Versioning not supported. File may be overwritten if it already exists."
        in capture_cli_logs.text
    )

    assert Path("output/abc/def").exists()
