"""Tests for IALiRTHKPathHandler."""

from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.io.file.IALiRTHKPathHandler import IALiRTHKPathHandler


def _handler(content_date=None) -> IALiRTHKPathHandler:
    return IALiRTHKPathHandler(content_date=content_date or datetime(2025, 6, 15))


class TestIALiRTHKPathHandler:
    def test_get_folder_structure_returns_year_month_path(self):
        assert _handler().get_folder_structure() == "ialirt_hk/2025/06"

    def test_get_filename_uses_date(self):
        assert _handler().get_filename() == "imap_ialirt_hk_20250615.csv"

    def test_get_full_path_combines_folder_and_filename(self):
        assert _handler().get_full_path() == Path(
            "ialirt_hk/2025/06/imap_ialirt_hk_20250615.csv"
        )

    def test_get_full_path_with_root(self):
        assert _handler().get_full_path(Path("datastore")) == Path(
            "datastore/ialirt_hk/2025/06/imap_ialirt_hk_20250615.csv"
        )

    def test_supports_sequencing_returns_false(self):
        assert _handler().supports_sequencing() is False

    def test_get_content_date_for_indexing_returns_content_date(self):
        dt = datetime(2025, 6, 15)
        assert _handler(dt).get_content_date_for_indexing() == dt

    def test_get_metadata_returns_none(self):
        assert _handler().get_metadata() is None

    def test_add_metadata_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            _handler().add_metadata({})

    def test_from_filename_parses_valid_filename(self):
        handler = IALiRTHKPathHandler.from_filename("imap_ialirt_hk_20250615.csv")
        assert handler is not None
        assert handler.content_date == datetime(2025, 6, 15)
        assert handler.extension == "csv"

    def test_from_filename_returns_none_for_non_matching(self):
        handler = IALiRTHKPathHandler.from_filename(
            "imap_mag_l2_norm-mago_20250615_v001.cdf"
        )
        assert handler is None

    def test_get_folder_structure_fails_without_content_date(self):
        with pytest.raises(ValueError):
            IALiRTHKPathHandler().get_folder_structure()

    def test_get_filename_fails_without_content_date(self):
        with pytest.raises(ValueError):
            IALiRTHKPathHandler().get_filename()

    @pytest.mark.parametrize(
        "date,expected_folder,expected_file",
        [
            (datetime(2025, 1, 1), "ialirt_hk/2025/01", "imap_ialirt_hk_20250101.csv"),
            (
                datetime(2025, 12, 31),
                "ialirt_hk/2025/12",
                "imap_ialirt_hk_20251231.csv",
            ),
        ],
    )
    def test_get_folder_and_filename_for_different_dates(
        self, date, expected_folder, expected_file
    ):
        handler = IALiRTHKPathHandler(content_date=date)
        assert handler.get_folder_structure() == expected_folder
        assert handler.get_filename() == expected_file
