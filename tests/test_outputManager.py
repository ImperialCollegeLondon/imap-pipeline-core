"""Tests for `OutputManager` class."""

from datetime import datetime
from pathlib import Path

from imap_mag.outputManager import OutputManager

from .testUtils import enableLogging, tidyDataFolders  # noqa: F401


def test_copy_new_file():
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = Path(".work/some_test_file.txt")
    original_file.parent.mkdir(parents=True, exist_ok=True)
    original_file.touch()

    # Exercise.
    manager.add_file(
        original_file,
        descriptor="pwr",
        date=datetime(2025, 5, 2),
        extension="txt",
    )

    # Verify.
    assert Path("output/2025/05/02/pwr-20250502-v000.txt").exists()


def test_copy_file_same_content():
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = Path(".work/some_test_file.txt")
    original_file.parent.mkdir(parents=True, exist_ok=True)
    original_file.touch()
    original_file.write_bytes(b"some content")

    existing_file = Path("output/2025/05/02/pwr-20250502-v000.txt")
    existing_file.parent.mkdir(parents=True, exist_ok=True)
    existing_file.touch()
    existing_file.write_bytes(b"some content")

    existing_modification_time = existing_file.stat().st_mtime

    # Exercise.
    manager.add_file(
        original_file,
        descriptor="pwr",
        date=datetime(2025, 5, 2),
        extension="txt",
    )

    # Verify.
    assert not Path("output/2025/05/02/pwr-20250502-v001.txt").exists()
    assert existing_file.stat().st_mtime == existing_modification_time


def test_copy_file_existing_versions():
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = Path(".work/some_test_file.txt")
    original_file.parent.mkdir(parents=True, exist_ok=True)
    original_file.touch()
    original_file.write_bytes(b"some content")

    for version in range(2):
        existing_file = Path(f"output/2025/05/02/pwr-20250502-v{version:03}.txt")
        existing_file.parent.mkdir(parents=True, exist_ok=True)
        existing_file.touch()

    # Exercise.
    manager.add_file(
        original_file,
        descriptor="pwr",
        date=datetime(2025, 5, 2),
        extension="txt",
    )

    # Verify.
    assert Path("output/2025/05/02/pwr-20250502-v002.txt").exists()


def test_copy_file_forced_version():
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = Path(".work/some_test_file.txt")
    original_file.parent.mkdir(parents=True, exist_ok=True)
    original_file.touch()

    # Exercise.
    manager.add_file(
        original_file,
        descriptor="pwr",
        date=datetime(2025, 5, 2),
        version="v003",
        extension="txt",
    )

    # Verify.
    assert Path("output/2025/05/02/pwr-20250502-v003.txt").exists()


def test_copy_file_custom_providers():
    # Set up.
    manager = OutputManager(
        Path("output"),
        folder_structure_provider=lambda **_: "abc",
        file_name_provider=lambda **_: "def",
    )

    original_file = Path(".work/some_test_file.txt")
    original_file.parent.mkdir(parents=True, exist_ok=True)
    original_file.touch()

    # Exercise.
    manager.add_file(original_file)

    # Verify.
    assert Path("output/abc/def").exists()
