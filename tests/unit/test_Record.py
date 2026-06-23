"""Tests for Record and FileRecord data classes."""

from datetime import datetime

import pytest

from imap_mag.data_pipelines.Record import FileRecord, Record


class TestRecord:
    def test_record_with_value(self):
        r = Record(value="hello")
        assert r.value == "hello"

    def test_record_with_kwargs(self):
        r = Record(start_date=datetime(2025, 1, 1), end_date=datetime(2025, 1, 31))
        assert r.start_date == datetime(2025, 1, 1)
        assert r.end_date == datetime(2025, 1, 31)

    def test_record_raises_without_value_or_kwargs(self):
        with pytest.raises(ValueError):
            Record()

    def test_record_repr_excludes_none_values(self):
        r = Record(value="test")
        repr_str = repr(r)
        assert "None" not in repr_str
        assert "test" in repr_str

    def test_file_record_stores_path_and_date(self, tmp_path):
        p = tmp_path / "test.cdf"
        p.touch()
        date = datetime(2025, 6, 1)
        fr = FileRecord(p, date)
        assert fr.file_path == p
        assert fr.content_date == date

    def test_file_record_value_is_filename(self, tmp_path):
        p = tmp_path / "myfile.cdf"
        p.touch()
        fr = FileRecord(p, datetime(2025, 1, 1))
        assert fr.value == "myfile.cdf"
