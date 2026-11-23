from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from imap_db.model import File, FileAnalysis, WorkflowProgress


def test_get_descriptor_from_filename():
    test_cases = [
        ("simplefile.txt", "simplefile"),
        ("complex_name_with_underscores_v001.docx", "complex_name_with_underscores"),
        ("imap_mag_l1_hsk-status_20251201_v001.csv", "imap_mag_l1_hsk-status"),
        ("imap_mag_l1_hsk-status_20251201_001.csv", "imap_mag_l1_hsk-status"),
        ("imap_ialirt_mag_20251201.csv", "imap_ialirt_mag"),
        (
            "imap_mag_l2-burst-offsets_20250421_20250421_v000.cdf",
            "imap_mag_l2-burst-offsets",
        ),
        ("imap_mag_l1d_burst-srf_20251207_v001.cdf", "imap_mag_l1d_burst-srf"),
        ("report_2023-05-01_v10.pdf", "report"),
        # New versioning format: _vMMM.mmmm suffix should be stripped
        (
            "imap_mag_l1a_norm-mago_20250101_v001.0001.cdf",
            "imap_mag_l1a_norm-mago",
        ),
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

    def test_preserves_version_major(self):
        original = _make_file(version_major=5)

        archived = original.archive_to_new_file_path(Path("/archive/test_file.cdf"))

        assert archived.version_major == 5


class TestFileMergeRecord:
    def test_merge_record_copies_version_major(self):
        original = _make_file(path="science/mag/l1a/test_file.cdf", version_major=1)
        updated = _make_file(
            path="science/mag/l1a/test_file.cdf", version_major=2, hash="newhash"
        )

        changed = original.merge_record(updated)

        assert changed is True
        assert original.version_major == 2

    def test_merge_record_returns_false_when_version_major_unchanged(self):
        original = _make_file(path="science/mag/l1a/test_file.cdf", version_major=1)
        same = _make_file(path="science/mag/l1a/test_file.cdf", version_major=1)

        changed = original.merge_record(same)

        assert changed is False

    def test_merge_record_logs_before_and_after_of_each_changed_field(self, caplog):
        path = "spice/ck/test_file.cdf"
        original = _make_file(
            path=path,
            size=100,
            hash="oldhash",
            file_meta={"ingestion_date": "2026-10-26", "kernel_type": "old_type"},
        )
        updated = _make_file(
            path=path,
            size=250,
            hash="newhash",
            file_meta={"ingestion_date": "2026-10-27", "kernel_type": "old_type"},
        )

        with caplog.at_level("INFO", logger="imap_db.model"):
            original.merge_record(updated)

        message = " ".join(r.getMessage() for r in caplog.records)
        assert "size: 100 -> 250" in message
        assert "hash: 'oldhash' -> 'newhash'" in message
        assert "file_meta.ingestion_date: '2026-10-26' -> '2026-10-27'" in message
        # Unchanged fields are not reported.
        assert "descriptor" not in message
        assert "kernel_type" not in message

    def test_merge_record_logs_added_and_removed_file_meta_keys(self, caplog):
        path = "spice/ck/test_file.cdf"
        original = _make_file(path=path, file_meta={"removed_key": 1})
        updated = _make_file(path=path, file_meta={"added_key": 2})

        with caplog.at_level("INFO", logger="imap_db.model"):
            original.merge_record(updated)

        message = " ".join(r.getMessage() for r in caplog.records)
        assert "file_meta.removed_key: 1 -> <missing>" in message
        assert "file_meta.added_key: <missing> -> 2" in message


class TestFileGetDatastoreRelativePath:
    def test_returns_relative_path_within_datastore(self):
        mock_settings = MagicMock()
        mock_settings.data_store = Path("/data/store")

        f = _make_file(path="/data/store/spice/ck/test.cdf")
        result = File.get_datastore_relative_path(Path(f.path), mock_settings)

        assert result == "spice/ck/test.cdf"

    def test_returns_original_path_when_outside_datastore(self):
        mock_settings = MagicMock()
        mock_settings.data_store = Path("/data/store")

        f = _make_file(path="/other/location/test.cdf")
        result = File.get_datastore_relative_path(Path(f.path), mock_settings)

        assert result == "/other/location/test.cdf"

    def test_no_warning_when_warn_false_and_outside_datastore(self, caplog):
        import logging

        mock_settings = MagicMock()
        mock_settings.data_store = Path("/data/store")

        f = _make_file(path="/other/location/test.cdf")
        with caplog.at_level(logging.WARNING):
            File.get_datastore_relative_path(Path(f.path), mock_settings, warn=False)

        assert not any(
            "is not within the data store path" in m for m in caplog.messages
        )

    def test_warning_when_warn_true_and_outside_datastore(self, caplog):
        import logging

        mock_settings = MagicMock()
        mock_settings.data_store = Path("/data/store")

        f = _make_file(path="/other/location/test.cdf")
        with caplog.at_level(logging.WARNING):
            File.get_datastore_relative_path(Path(f.path), mock_settings, warn=True)

        assert any("is not within the data store path" in m for m in caplog.messages)


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

    def test_version_major_is_stored_when_provided(self, tmp_path):
        mock_settings = MagicMock()
        mock_settings.data_store = tmp_path

        test_file = tmp_path / "test.cdf"
        test_file.write_bytes(b"content")

        f = File.from_file(
            test_file,
            version=1,
            hash=None,
            content_date=datetime(2025, 1, 1),
            settings=mock_settings,
            version_major=3,
        )

        assert f.version_major == 3


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

    def test_without_version_major_filter_returns_latest_version_across_all_majors(
        self,
    ):
        files = [
            _make_file(
                descriptor="mag-l1a",
                version=1,
                version_major=1,
                content_date=datetime(2025, 1, 1),
            ),
            _make_file(
                descriptor="mag-l1a",
                version=2,
                version_major=2,
                content_date=datetime(2025, 1, 1),
            ),
        ]

        result = File.filter_to_latest_versions_only(files)

        assert len(result) == 1
        assert result[0].version == 2

    def test_with_version_major_filter_restricts_to_that_major(self):
        files = [
            _make_file(
                descriptor="mag-l1a",
                version=1,
                version_major=1,
                content_date=datetime(2025, 1, 1),
            ),
            _make_file(
                descriptor="mag-l1a",
                version=2,
                version_major=2,
                content_date=datetime(2025, 1, 1),
            ),
        ]

        result = File.filter_to_latest_versions_only(files, version_major=1)

        assert len(result) == 1
        assert result[0].version == 1
        assert result[0].version_major == 1


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


class TestFileAnalysisGapInWindow:
    WINDOW_START = datetime(2026, 1, 29, 23, 59)
    WINDOW_END = datetime(2026, 1, 31, 0, 1)

    @staticmethod
    def _gap(start: str, end: str) -> dict:
        return {"start_time": start, "end_time": end, "duration_seconds": 0}

    def _analysis(self, **kwargs) -> FileAnalysis:
        # Defaults comfortably cover the window, so each test isolates one cause.
        fields = {
            "first_timestamp": datetime(2026, 1, 29),
            "last_timestamp": datetime(2026, 2, 1),
            "gaps": [],
        }
        fields.update(kwargs)
        return FileAnalysis(**fields)

    def _gap_in_window(self, analysis: FileAnalysis) -> bool:
        return analysis.gap_in_window(self.WINDOW_START, self.WINDOW_END)

    def test_false_when_coverage_spans_the_window_with_no_gaps(self):
        assert self._gap_in_window(self._analysis()) is False

    def test_false_when_gaps_is_none(self):
        assert self._gap_in_window(self._analysis(gaps=None)) is False

    def test_false_when_coverage_exactly_matches_the_window(self):
        analysis = self._analysis(
            first_timestamp=self.WINDOW_START, last_timestamp=self.WINDOW_END
        )
        assert self._gap_in_window(analysis) is False

    def test_false_when_recorded_gap_is_outside_the_window(self):
        analysis = self._analysis(
            gaps=[self._gap("2026-02-15T10:00:00", "2026-02-15T10:05:00")]
        )
        assert self._gap_in_window(analysis) is False

    def test_false_when_gap_ends_exactly_at_window_start(self):
        analysis = self._analysis(
            gaps=[self._gap("2026-01-29T23:00:00", "2026-01-29T23:59:00")]
        )
        assert self._gap_in_window(analysis) is False

    def test_true_when_recorded_gap_overlaps_the_window(self):
        analysis = self._analysis(
            gaps=[self._gap("2026-01-30T10:00:00", "2026-01-30T10:05:00")]
        )
        assert self._gap_in_window(analysis) is True

    def test_true_when_recorded_gap_only_partially_overlaps_the_window(self):
        # Starts before the window and ends just inside it.
        analysis = self._analysis(
            gaps=[self._gap("2026-01-29T20:00:00", "2026-01-30T00:00:01")]
        )
        assert self._gap_in_window(analysis) is True

    @pytest.mark.parametrize(
        ("first", "last"),
        [(None, None), (None, datetime(2026, 2, 1)), (datetime(2026, 1, 29), None)],
        ids=["both-missing", "first-missing", "last-missing"],
    )
    def test_true_when_timestamp_range_not_recorded(self, first, last):
        # Coverage can't be confirmed, so treat as incomplete.
        analysis = self._analysis(first_timestamp=first, last_timestamp=last)
        assert self._gap_in_window(analysis) is True

    def test_true_when_data_starts_after_the_window_start(self):
        # A leading gap that `gaps` can't see - no earlier record to diff against.
        analysis = self._analysis(first_timestamp=datetime(2026, 1, 30, 6, 0))
        assert self._gap_in_window(analysis) is True

    def test_true_when_data_ends_before_the_window_end(self):
        # A trailing gap that `gaps` can't see - no later record to diff against.
        analysis = self._analysis(last_timestamp=datetime(2026, 1, 30, 18, 0))
        assert self._gap_in_window(analysis) is True

    def test_timezone_aware_timestamps_compared_as_utc(self):
        # Timestamps loaded from the database are timezone-aware; they must still
        # compare against the naive window without raising.
        analysis = self._analysis(
            first_timestamp=datetime(2026, 1, 29, tzinfo=UTC),
            last_timestamp=datetime(2026, 2, 1, tzinfo=UTC),
            gaps=[self._gap("2026-01-30T10:00:00+00:00", "2026-01-30T10:05:00+00:00")],
        )
        assert self._gap_in_window(analysis) is True
