"""Tests for SpinTablePathHandler."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest


class TestSpinTablePathHandlerFromFilename:
    def test_parses_valid_spin_filename(self):
        from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler

        handler = SpinTablePathHandler.from_filename(
            "imap_2026_089_2026_090_01.spin"
        )

        assert handler is not None
        assert handler.version == 1
        assert handler.content_date == datetime(2026, 3, 30)  # 2026 day 089

    def test_returns_none_for_invalid_filename(self):
        from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler

        result = SpinTablePathHandler.from_filename("not_a_spin_file.csv")
        assert result is None

    def test_returns_none_for_none_input(self):
        from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler

        result = SpinTablePathHandler.from_filename(None)
        assert result is None

    def test_parses_path_object(self):
        from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler

        path = Path("/some/dir/imap_2025_001_2025_001_03.spin")
        handler = SpinTablePathHandler.from_filename(path)

        assert handler is not None
        assert handler.version == 3
        assert handler.filename == "imap_2025_001_2025_001_03.spin"

    def test_filename_is_set_correctly(self):
        from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler

        handler = SpinTablePathHandler.from_filename("imap_2025_100_2025_101_02.spin")

        assert handler.filename == "imap_2025_100_2025_101_02.spin"
        assert handler.get_filename() == "imap_2025_100_2025_101_02.spin"


class TestSpinTablePathHandlerAddMetadata:
    def test_add_metadata_sets_content_date_from_start_date(self):
        from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler

        handler = SpinTablePathHandler.from_filename("imap_2025_100_2025_101_01.spin")
        handler.add_metadata({"start_date": "20250410", "version": "2"})

        assert handler.content_date == datetime(2025, 4, 10)
        assert handler.version == 2

    def test_add_metadata_falls_back_to_ingestion_date(self):
        from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler

        handler = SpinTablePathHandler.from_filename("imap_2025_100_2025_101_01.spin")
        handler.add_metadata(
            {"ingestion_date": "2025-04-15T12:00:00", "version": "1"}
        )

        assert handler.content_date == datetime(2025, 4, 15, 12, 0, 0)

    def test_add_metadata_stores_full_metadata(self):
        from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler

        handler = SpinTablePathHandler.from_filename("imap_2025_100_2025_101_01.spin")
        meta = {"start_date": "20250410", "version": "1", "extra": "data"}
        handler.add_metadata(meta)

        assert handler.get_metadata() == meta


class TestSpinTablePathHandlerFolderStructure:
    def test_get_folder_structure_returns_spice_spin(self):
        from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler

        handler = SpinTablePathHandler.from_filename("imap_2025_100_2025_101_01.spin")

        assert "spice" in handler.get_folder_structure()
        assert "spin" in handler.get_folder_structure()

    def test_get_root_folder_is_spice(self):
        from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler

        assert SpinTablePathHandler.get_root_folder() == "spice"

    def test_does_not_support_sequencing(self):
        from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler

        handler = SpinTablePathHandler.from_filename("imap_2025_100_2025_101_01.spin")
        assert handler.supports_sequencing() is False

    def test_get_unsequenced_pattern_raises(self):
        from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler

        handler = SpinTablePathHandler.from_filename("imap_2025_100_2025_101_01.spin")
        with pytest.raises(ValueError, match="do not support sequencing"):
            handler.get_unsequenced_pattern()

    def test_get_content_date_for_indexing_returns_content_date(self):
        from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler

        handler = SpinTablePathHandler.from_filename("imap_2026_089_2026_090_01.spin")

        content_date = handler.get_content_date_for_indexing()
        assert content_date is not None
        assert content_date.year == 2026
