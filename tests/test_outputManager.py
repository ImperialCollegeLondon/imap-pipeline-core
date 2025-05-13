"""Tests for `OutputManager` class."""

from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.io import (
    IFileMetadataProvider,
    OutputManager,
    StandardSPDFMetadataProvider,
)
from tests.util.miscellaneous import (  # noqa: F401
    create_test_file,
    enableLogging,
    tidyDataFolders,
)


def test_copy_new_file():
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = create_test_file(Path(".work/some_test_file.txt"))

    # Exercise.
    manager.add_spdf_format_file(
        original_file,
        descriptor="pwr",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )

    # Verify.
    assert Path("output/2025/05/02/imap_mag_pwr_20250502_v000.txt").exists()


def test_copy_file_same_content():
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = create_test_file(Path(".work/some_test_file.txt"), "some content")
    existing_file = create_test_file(
        Path("output/2025/05/02/imap_mag_pwr_20250502_v000.txt"), "some content"
    )

    existing_modification_time = existing_file.stat().st_mtime

    # Exercise.
    manager.add_spdf_format_file(
        original_file,
        descriptor="pwr",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )

    # Verify.
    assert not Path("output/2025/05/02/imap_mag_pwr_20250502_v001.txt").exists()
    assert existing_file.stat().st_mtime == existing_modification_time


def test_copy_file_existing_versions():
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = create_test_file(Path(".work/some_test_file.txt"), "some content")

    for version in range(2):
        create_test_file(
            Path(f"output/2025/05/02/imap_mag_pwr_20250502_v{version:03}.txt")
        )

    # Exercise.
    manager.add_spdf_format_file(
        original_file,
        descriptor="pwr",
        content_date=datetime(2025, 5, 2),
        extension="txt",
    )

    # Verify.
    assert Path("output/2025/05/02/imap_mag_pwr_20250502_v002.txt").exists()


def test_copy_file_forced_version():
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = create_test_file(Path(".work/some_test_file.txt"))

    # Exercise.
    manager.add_spdf_format_file(
        original_file,
        descriptor="pwr",
        content_date=datetime(2025, 5, 2),
        version=3,
        extension="txt",
    )

    # Verify.
    assert Path("output/2025/05/02/imap_mag_pwr_20250502_v003.txt").exists()


class TestMetadataProvider(IFileMetadataProvider):
    def supports_versioning(self) -> bool:
        return False

    def get_folder_structure(self) -> str:
        return "abc"

    def get_filename(self) -> str:
        return "def"


def test_copy_file_custom_providers():
    # Set up.
    manager = OutputManager(Path("output"))

    original_file = create_test_file(Path(".work/some_test_file.txt"))

    # Exercise.
    manager.add_file(original_file, TestMetadataProvider())

    # Verify.
    assert Path("output/abc/def").exists()


@pytest.mark.parametrize(
    "filename, expected",
    [
        (
            "imap_mag_hsk-pw_20241210_v003.pkts",
            StandardSPDFMetadataProvider(
                descriptor="hsk-pw",
                content_date=datetime(2024, 12, 10),
                version=3,
                extension="pkts",
            ),
        ),
        (
            "imap_mag_l1b_mago-normal_20250502_v001.cdf",
            StandardSPDFMetadataProvider(
                level="l1b",
                descriptor="mago-normal",
                content_date=datetime(2025, 5, 2),
                version=1,
                extension="cdf",
            ),
        ),
        (
            "imap_mag_l2_burst_20261231_v010.cdf",
            StandardSPDFMetadataProvider(
                level="l2",
                descriptor="burst",
                content_date=datetime(2026, 12, 31),
                version=10,
                extension="cdf",
            ),
        ),
        (
            "imap_mag_definitely_not_a_standard_spdf_file.txt",
            None,
        ),
    ],
)
def test_standard_spdf_metadata_provider_from_filename(filename, expected):
    actual = StandardSPDFMetadataProvider.from_filename(filename)
    assert actual == expected
