"""Tests for IALiRTQuicklookPathHandler and LatestFilePathHandler."""

from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.io.file.IALiRTQuicklookPathHandler import IALiRTQuicklookPathHandler
from imap_mag.io.file.LatestFilePathHandler import LatestFilePathHandler


class TestLatestFilePathHandler:
    def _handler(self) -> LatestFilePathHandler:
        return LatestFilePathHandler(root=Path("quicklook/ialirt"), extension="png")

    def test_get_folder_structure_returns_root(self):
        assert self._handler().get_folder_structure() == "quicklook/ialirt"

    def test_get_filename_returns_latest_with_extension(self):
        assert self._handler().get_filename() == "latest.png"

    def test_get_full_path_combines_root_and_latest(self):
        assert self._handler().get_full_path() == Path("quicklook/ialirt/latest.png")

    def test_get_full_path_with_root(self):
        assert self._handler().get_full_path(Path("datastore")) == Path(
            "datastore/quicklook/ialirt/latest.png"
        )

    def test_supports_sequencing_returns_false(self):
        assert self._handler().supports_sequencing() is False

    def test_get_content_date_for_indexing_returns_none_by_default(self):
        assert self._handler().get_content_date_for_indexing() is None

    def test_get_content_date_for_indexing_returns_provided_date(self):
        dt = datetime(2025, 6, 1)
        handler = LatestFilePathHandler(
            root=Path("quicklook"), extension="png", latest_date=dt
        )
        assert handler.get_content_date_for_indexing() == dt

    def test_get_metadata_returns_none(self):
        assert self._handler().get_metadata() is None

    def test_add_metadata_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            self._handler().add_metadata({})

    def test_from_filename_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            LatestFilePathHandler.from_filename("latest.png")

    def test_get_folder_structure_fails_without_root(self):
        with pytest.raises(ValueError):
            LatestFilePathHandler(extension="png").get_folder_structure()

    def test_get_filename_fails_without_extension(self):
        with pytest.raises(ValueError):
            LatestFilePathHandler(root=Path("some/folder")).get_filename()


class TestIALiRTQuicklookPathHandler:
    def _handler(self, content_date=None) -> IALiRTQuicklookPathHandler:
        return IALiRTQuicklookPathHandler(
            content_date=content_date or datetime(2025, 6, 15)
        )

    def test_get_plot_type_returns_ialirt(self):
        assert IALiRTQuicklookPathHandler.get_plot_type() == "ialirt"

    def test_get_filename_uses_date(self):
        assert self._handler().get_filename() == "imap_quicklook_ialirt_20250615.png"

    def test_get_folder_structure_contains_ialirt(self):
        assert "ialirt" in self._handler().get_folder_structure()

    def test_get_full_path_combines_folder_and_filename(self):
        handler = self._handler()
        full = handler.get_full_path()
        assert full == Path(
            "quicklook/ialirt/2025/06/imap_quicklook_ialirt_20250615.png"
        )

    def test_from_filename_parses_valid_filename(self):
        handler = IALiRTQuicklookPathHandler.from_filename(
            "imap_quicklook_ialirt_20250615.png"
        )
        assert handler is not None
        assert handler.content_date == datetime(2025, 6, 15)

    def test_from_filename_returns_none_for_non_matching(self):
        handler = IALiRTQuicklookPathHandler.from_filename(
            "imap_quicklook_hk_20250615.png"
        )
        assert handler is None

    def test_supports_sequencing_returns_false(self):
        assert self._handler().supports_sequencing() is False

    def test_get_folder_structure_fails_without_content_date(self):
        with pytest.raises(ValueError):
            IALiRTQuicklookPathHandler().get_folder_structure()
