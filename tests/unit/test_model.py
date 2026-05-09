from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from imap_db.model import File, WorkflowProgress


def test_get_descriptor_from_filename():
    test_cases = [
        ("simplefile.txt", "simplefile"),
        ("complex_name_with_underscores_v001.docx", "complex_name_with_underscores"),
        ("imap_mag_l1_hsk-status_20251201_v001.csv", "imap_mag_l1_hsk-status"),
        ("imap_mag_l1_hsk-status_20251201_001.csv", "imap_mag_l1_hsk-status"),
        ("imap_ialirt_20251201.csv", "imap_ialirt"),
        (
            "imap_mag_l2-burst-offsets_20250421_20250421_v000.cdf",
            "imap_mag_l2-burst-offsets",
        ),
        ("imap_mag_l1d_burst-srf_20251207_v001.cdf", "imap_mag_l1d_burst-srf"),
        ("report_2023-05-01_v10.pdf", "report"),
    ]

    for filename, expected_descriptor in test_cases:
        descriptor = File.get_descriptor_from_filename(filename)
        assert descriptor == expected_descriptor, f"Failed for filename: {filename}"


def _make_file(**kwargs):
    defaults = {
        "name": "test_file.cdf",
        "path": "spice/ck/test_file.cdf",
        "descriptor": "test_desc",
        "version": 1,
        "hash": "abc123",
        "size": 100,
        "content_date": datetime(2025, 1, 1),
        "software_version": "1.0.0",
    }
    defaults.update(kwargs)
    return File(**defaults)


class TestFileSetDeleted:
    def test_set_deleted_sets_deletion_date(self):
        f = _make_file()
        assert f.deletion_date is None

        f.set_deleted()

        assert f.deletion_date is not None

    def test_set_deleted_updates_last_modified_date(self):
        f = _make_file()
        f.set_deleted()
        assert f.last_modified_date is not None


class TestFileArchiveToNewFilePath:
    def test_creates_new_file_with_new_path(self):
        original = _make_file()
        new_path = Path("/new/location/test_file.cdf")

        archived = original.archive_to_new_file_path(new_path)

        assert archived.path == new_path.as_posix()

    def test_creates_new_file_with_same_descriptor_and_version(self):
        original = _make_file(descriptor="mag-l1a", version=3)
        archived = original.archive_to_new_file_path(Path("/archive/test_file.cdf"))

        assert archived.descriptor == "mag-l1a"
        assert archived.version == 3

    def test_marks_original_file_as_deleted(self):
        original = _make_file()
        assert original.deletion_date is None

        original.archive_to_new_file_path(Path("/archive/test_file.cdf"))

        assert original.deletion_date is not None


class TestFileGetDatastoreRelativePath:
    def test_returns_relative_path_within_datastore(self):
        mock_settings = MagicMock()
        mock_settings.data_store = Path("/data/store")

        f = _make_file(path="/data/store/spice/ck/test.cdf")
        result = f.get_datastore_relative_path(mock_settings)

        assert result == Path("spice/ck/test.cdf")

    def test_returns_original_path_when_outside_datastore(self):
        mock_settings = MagicMock()
        mock_settings.data_store = Path("/data/store")

        f = _make_file(path="/other/location/test.cdf")
        result = f.get_datastore_relative_path(mock_settings)

        assert result == Path("/other/location/test.cdf")


class TestFileFromFile:
    def test_creates_file_from_existing_path(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path

        test_file = tmp_path / "test.cdf"
        test_file.write_bytes(b"test content")

        f = File.from_file(
            test_file,
            version=1,
            hash=None,
            content_date=datetime(2025, 1, 1),
            settings=mock_settings,
        )

        assert f.name == "test.cdf"
        assert f.version == 1
        assert f.content_date == datetime(2025, 1, 1)
        assert f.size == len(b"test content")

    def test_raises_for_nonexistent_file(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path

        with pytest.raises(FileNotFoundError):
            File.from_file(
                tmp_path / "nonexistent.cdf",
                version=1,
                hash=None,
                content_date=None,
                settings=mock_settings,
            )

    def test_computes_hash_when_not_provided(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path

        test_file = tmp_path / "test.cdf"
        test_file.write_bytes(b"content")

        f = File.from_file(
            test_file, version=1, hash=None, content_date=None, settings=mock_settings
        )

        assert f.hash is not None
        assert len(f.hash) > 0

    def test_uses_provided_hash(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path

        test_file = tmp_path / "test.cdf"
        test_file.write_bytes(b"content")

        f = File.from_file(
            test_file,
            version=1,
            hash="my_custom_hash",
            content_date=None,
            settings=mock_settings,
        )

        assert f.hash == "my_custom_hash"

    def test_path_is_relative_to_datastore_when_within_it(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path

        sub_dir = tmp_path / "subdir"
        sub_dir.mkdir()
        test_file = sub_dir / "test.cdf"
        test_file.write_bytes(b"content")

        f = File.from_file(
            test_file, version=1, hash=None, content_date=None, settings=mock_settings
        )

        assert "subdir" in f.path
        assert str(tmp_path) not in f.path


class TestFileFilterToLatestVersionsOnly:
    def test_selects_latest_version_per_date(self):
        files = [
            _make_file(
                descriptor="mag-l1a", version=1, content_date=datetime(2025, 1, 1)
            ),
            _make_file(
                descriptor="mag-l1a", version=2, content_date=datetime(2025, 1, 1)
            ),
            _make_file(
                descriptor="mag-l1a", version=3, content_date=datetime(2025, 1, 1)
            ),
        ]

        result = File.filter_to_latest_versions_only(files)

        assert len(result) == 1
        assert result[0].version == 3

    def test_keeps_different_dates_separately(self):
        files = [
            _make_file(
                descriptor="mag-l1a", version=1, content_date=datetime(2025, 1, 1)
            ),
            _make_file(
                descriptor="mag-l1a", version=1, content_date=datetime(2025, 1, 2)
            ),
        ]

        result = File.filter_to_latest_versions_only(files)

        assert len(result) == 2

    def test_handles_files_without_content_date(self):
        files = [
            _make_file(descriptor="mag-l1a", version=1, content_date=None),
            _make_file(descriptor="mag-l1a", version=2, content_date=None),
        ]

        result = File.filter_to_latest_versions_only(files)

        assert len(result) == 1
        assert result[0].version == 2

    def test_handles_mixed_dates_and_no_dates(self):
        files = [
            _make_file(
                descriptor="mag-l1a", version=1, content_date=datetime(2025, 1, 1)
            ),
            _make_file(descriptor="mag-l1a", version=1, content_date=None),
        ]

        result = File.filter_to_latest_versions_only(files)

        assert len(result) == 2


class TestWorkflowProgress:
    def test_get_item_name(self):
        wp = WorkflowProgress(item_name="test_item")
        assert wp.get_item_name() == "test_item"

    def test_get_progress_timestamp_returns_none_by_default(self):
        wp = WorkflowProgress(item_name="test_item")
        assert wp.get_progress_timestamp() is None

    def test_update_progress_timestamp(self):
        wp = WorkflowProgress(item_name="test_item")
        ts = datetime(2025, 1, 15, 10, 30, 0)

        wp.update_progress_timestamp(ts)

        assert wp.get_progress_timestamp() == ts

    def test_update_last_checked_timestamp(self):
        wp = WorkflowProgress(item_name="test_item")
        ts = datetime(2025, 1, 15, 10, 30, 0)

        wp.update_last_checked_timestamp(ts)

        assert wp.get_last_checked_date() == ts
