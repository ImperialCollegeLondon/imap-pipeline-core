"""Tests for `OutputManager` class."""

from datetime import datetime
from pathlib import Path

from imap_mag.outputManager import IMetadataProvider, OutputManager

from .testUtils import enableLogging, tidyDataFolders  # noqa: F401


def test_copy_new_file():
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = Path(".work/some_test_file.txt")
    original_file.parent.mkdir(parents=True, exist_ok=True)
    original_file.touch()

    # Exercise.
    manager.add_default_file(
        original_file,
        descriptor="pwr",
        date=datetime(2025, 5, 2),
        extension="txt",
    )

    # Verify.
    assert Path("output/2025/05/02/pwr_20250502_v000.txt").exists()


def test_copy_file_same_content():
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = Path(".work/some_test_file.txt")
    original_file.parent.mkdir(parents=True, exist_ok=True)
    original_file.touch()
    original_file.write_bytes(b"some content")

    existing_file = Path("output/2025/05/02/pwr_20250502_v000.txt")
    existing_file.parent.mkdir(parents=True, exist_ok=True)
    existing_file.touch()
    existing_file.write_bytes(b"some content")

    existing_modification_time = existing_file.stat().st_mtime

    # Exercise.
    manager.add_default_file(
        original_file,
        descriptor="pwr",
        date=datetime(2025, 5, 2),
        extension="txt",
    )

    # Verify.
    assert not Path("output/2025/05/02/pwr_20250502_v001.txt").exists()
    assert existing_file.stat().st_mtime == existing_modification_time


def test_copy_file_existing_versions():
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = Path(".work/some_test_file.txt")
    original_file.parent.mkdir(parents=True, exist_ok=True)
    original_file.touch()
    original_file.write_bytes(b"some content")

    for version in range(2):
        existing_file = Path(f"output/2025/05/02/pwr_20250502_v{version:03}.txt")
        existing_file.parent.mkdir(parents=True, exist_ok=True)
        existing_file.touch()

    # Exercise.
    manager.add_default_file(
        original_file,
        descriptor="pwr",
        date=datetime(2025, 5, 2),
        extension="txt",
    )

    # Verify.
    assert Path("output/2025/05/02/pwr_20250502_v002.txt").exists()


def test_copy_file_forced_version():
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = Path(".work/some_test_file.txt")
    original_file.parent.mkdir(parents=True, exist_ok=True)
    original_file.touch()

    # Exercise.
    manager.add_default_file(
        original_file,
        descriptor="pwr",
        date=datetime(2025, 5, 2),
        version=3,
        extension="txt",
    )

    # Verify.
    assert Path("output/2025/05/02/pwr_20250502_v003.txt").exists()


class TestMetadataProvider(IMetadataProvider):
    def get_folder_structure(self) -> str:
        return "abc"

    def get_file_name(self) -> str:
        return "def"


def test_copy_file_custom_providers():
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = Path(".work/some_test_file.txt")
    original_file.parent.mkdir(parents=True, exist_ok=True)
    original_file.touch()

    # Exercise.
    manager.add_file(original_file, TestMetadataProvider())

    # Verify.
    assert Path("output/abc/def").exists()
