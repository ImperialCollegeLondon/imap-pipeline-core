"""Tests for SpinTablePathHandler."""

from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler


class TestSpinTablePathHandlerFromFilename:
    def test_returns_none_for_none_input(self):
        result = SpinTablePathHandler.from_filename(None)
        assert result is None

    def test_parses_path_object(self):
        path = Path("/some/dir/imap_2025_001_2025_001_03.spin")
        handler = SpinTablePathHandler.from_filename(path)

        assert handler is not None
        assert handler.version == 3
        assert handler.filename == "imap_2025_001_2025_001_03.spin"

    def test_filename_is_set_correctly(self):
        handler = SpinTablePathHandler.from_filename("imap_2025_100_2025_101_02.spin")

        assert handler.filename == "imap_2025_100_2025_101_02.spin"
        assert handler.get_filename() == "imap_2025_100_2025_101_02.spin"


class TestSpinTablePathHandlerAddMetadata:
    def test_add_metadata_sets_content_date_from_start_date(self):
        handler = SpinTablePathHandler.from_filename("imap_2025_100_2025_101_01.spin")
        handler.add_metadata({"start_date": "20250410", "version": "2"})

        assert handler.content_date == datetime(2025, 4, 10)
        assert handler.version == 2

    def test_add_metadata_falls_back_to_ingestion_date(self):
        handler = SpinTablePathHandler.from_filename("imap_2025_100_2025_101_01.spin")
        handler.add_metadata({"ingestion_date": "2025-04-15T12:00:00", "version": "1"})

        assert handler.content_date == datetime(2025, 4, 15, 12, 0, 0)

    def test_add_metadata_stores_full_metadata(self):
        handler = SpinTablePathHandler.from_filename("imap_2025_100_2025_101_01.spin")
        meta = {"start_date": "20250410", "version": "1", "extra": "data"}
        handler.add_metadata(meta)

        assert handler.get_metadata() == meta


class TestSpinTablePathHandlerFolderStructure:
    def test_get_folder_structure_returns_spice_spin(self):
        handler = SpinTablePathHandler.from_filename("imap_2025_100_2025_101_01.spin")

        assert "spice" in handler.get_folder_structure()
        assert "spin" in handler.get_folder_structure()

    def test_get_root_folder_is_spice(self):
        assert SpinTablePathHandler.get_root_folder() == "spice"

    def test_does_not_support_sequencing(self):
        handler = SpinTablePathHandler.from_filename("imap_2025_100_2025_101_01.spin")
        assert handler.supports_sequencing() is False

    def test_get_unsequenced_pattern_raises(self):
        handler = SpinTablePathHandler.from_filename("imap_2025_100_2025_101_01.spin")
        with pytest.raises(ValueError, match="do not support sequencing"):
            handler.get_unsequenced_pattern()

    def test_get_content_date_for_indexing_returns_content_date(self):
        handler = SpinTablePathHandler.from_filename("imap_2026_089_2026_090_01.spin")

        content_date = handler.get_content_date_for_indexing()
        assert content_date is not None
        assert content_date.year == 2026
