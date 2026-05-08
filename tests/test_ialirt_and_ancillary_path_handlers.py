"""Tests for IALiRTHKPathHandler, AncillaryPathHandler, LatestFilePathHandler and QuicklookPathHandler."""

from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.io.file.AncillaryPathHandler import AncillaryPathHandler
from imap_mag.io.file.IALiRTHKPathHandler import IALiRTHKPathHandler
from imap_mag.io.file.IALiRTQuicklookPathHandler import IALiRTQuicklookPathHandler
from imap_mag.io.file.LatestFilePathHandler import LatestFilePathHandler


class TestIALiRTHKPathHandler:
    def _handler(self, content_date=None) -> IALiRTHKPathHandler:
        return IALiRTHKPathHandler(content_date=content_date or datetime(2025, 6, 15))

    def test_get_folder_structure_returns_year_month_path(self):
        assert self._handler().get_folder_structure() == "ialirt_hk/2025/06"

    def test_get_filename_uses_date(self):
        assert self._handler().get_filename() == "imap_ialirt_hk_20250615.csv"

    def test_get_full_path_combines_folder_and_filename(self):
        handler = self._handler()
        assert handler.get_full_path() == Path("ialirt_hk/2025/06/imap_ialirt_hk_20250615.csv")

    def test_get_full_path_with_root(self):
        handler = self._handler()
        assert handler.get_full_path(Path("datastore")) == Path(
            "datastore/ialirt_hk/2025/06/imap_ialirt_hk_20250615.csv"
        )

    def test_supports_sequencing_returns_false(self):
        assert self._handler().supports_sequencing() is False

    def test_get_content_date_for_indexing_returns_content_date(self):
        dt = datetime(2025, 6, 15)
        assert self._handler(dt).get_content_date_for_indexing() == dt

    def test_get_metadata_returns_none(self):
        assert self._handler().get_metadata() is None

    def test_add_metadata_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            self._handler().add_metadata({})

    def test_from_filename_parses_valid_filename(self):
        handler = IALiRTHKPathHandler.from_filename("imap_ialirt_hk_20250615.csv")
        assert handler is not None
        assert handler.content_date == datetime(2025, 6, 15)
        assert handler.extension == "csv"

    def test_from_filename_returns_none_for_non_matching(self):
        handler = IALiRTHKPathHandler.from_filename("imap_mag_l2_norm-mago_20250615_v001.cdf")
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
            (datetime(2025, 12, 31), "ialirt_hk/2025/12", "imap_ialirt_hk_20251231.csv"),
        ],
    )
    def test_get_folder_and_filename_for_different_dates(self, date, expected_folder, expected_file):
        handler = IALiRTHKPathHandler(content_date=date)
        assert handler.get_folder_structure() == expected_folder
        assert handler.get_filename() == expected_file


class TestAncillaryPathHandler:
    def _offsets_handler(self, start=None, end=None, version=1) -> AncillaryPathHandler:
        return AncillaryPathHandler(
            descriptor="l2-norm-offsets",
            start_date=start or datetime(2025, 10, 17),
            end_date=end,
            version=version,
            extension="cdf",
        )

    def test_get_folder_structure_for_offsets_file(self):
        folder = self._offsets_handler().get_folder_structure()
        assert "science-ancillary" in folder
        assert "l2-offsets" in folder

    def test_get_filename_with_end_date(self):
        handler = self._offsets_handler(
            start=datetime(2025, 10, 17),
            end=datetime(2025, 10, 20),
            version=2,
        )
        assert handler.get_filename() == "imap_mag_l2-norm-offsets_20251017_20251020_v002.cdf"

    def test_get_filename_without_end_date(self):
        handler = self._offsets_handler(start=datetime(2025, 10, 17))
        assert handler.get_filename() == "imap_mag_l2-norm-offsets_20251017_v001.cdf"

    def test_get_content_date_for_indexing_returns_start_date_for_offsets(self):
        dt = datetime(2025, 10, 17)
        handler = self._offsets_handler(start=dt)
        assert handler.get_content_date_for_indexing() == dt

    def test_get_content_date_for_indexing_returns_none_for_calibration(self):
        handler = AncillaryPathHandler(
            descriptor="l2-calibration",
            start_date=datetime(2025, 10, 17),
            version=1,
            extension="cdf",
        )
        assert handler.get_content_date_for_indexing() is None

    @pytest.mark.parametrize(
        "descriptor,expected_subfolder",
        [
            ("ialirt-calibration", "ialirt"),
            ("l1d-calibration", "l1d"),
            ("l2-calibration", "l2-rotation"),
            ("l1b-calibration", "l1b"),
        ],
    )
    def test_get_sub_folder_for_known_descriptors(self, descriptor, expected_subfolder):
        handler = AncillaryPathHandler(
            descriptor=descriptor,
            start_date=datetime(2025, 10, 17),
            version=1,
            extension="cdf",
        )
        sub = handler.get_sub_folder()
        assert sub == Path(expected_subfolder)

    def test_get_sub_folder_for_unknown_descriptor_raises_value_error(self):
        handler = AncillaryPathHandler(
            descriptor="unknown-type",
            start_date=datetime(2025, 10, 17),
            version=1,
            extension="cdf",
        )
        with pytest.raises(ValueError, match="Unknown descriptor"):
            handler.get_sub_folder()

    def test_get_unsequenced_pattern_matches_versioned_filename(self):
        handler = self._offsets_handler(
            start=datetime(2025, 10, 17),
            end=datetime(2025, 10, 20),
        )
        pattern = handler.get_unsequenced_pattern()
        assert pattern.match("imap_mag_l2-norm-offsets_20251017_20251020_v001.cdf")
        assert pattern.match("imap_mag_l2-norm-offsets_20251017_20251020_v042.cdf")
        assert not pattern.match("imap_mag_l2-norm-offsets_20251017_v001.cdf")

    def test_from_filename_parses_with_end_date(self):
        handler = AncillaryPathHandler.from_filename(
            "imap_mag_l2-norm-offsets_20251017_20251020_v001.cdf"
        )
        assert handler is not None
        assert handler.descriptor == "l2-norm-offsets"
        assert handler.start_date == datetime(2025, 10, 17)
        assert handler.end_date == datetime(2025, 10, 20)
        assert handler.version == 1
        assert handler.extension == "cdf"

    def test_from_filename_parses_without_end_date(self):
        handler = AncillaryPathHandler.from_filename(
            "imap_mag_l2-calibration_20251017_v001.cdf"
        )
        assert handler is not None
        assert handler.descriptor == "l2-calibration"
        assert handler.end_date is None

    def test_from_filename_returns_none_for_non_matching(self):
        handler = AncillaryPathHandler.from_filename("imap_mag_l2_norm-mago_20251017_v001.cdf")
        assert handler is None


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
        handler = LatestFilePathHandler(root=Path("quicklook"), extension="png", latest_date=dt)
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
        return IALiRTQuicklookPathHandler(content_date=content_date or datetime(2025, 6, 15))

    def test_get_plot_type_returns_ialirt(self):
        assert IALiRTQuicklookPathHandler.get_plot_type() == "ialirt"

    def test_get_filename_uses_date(self):
        assert self._handler().get_filename() == "imap_quicklook_ialirt_20250615.png"

    def test_get_folder_structure_contains_ialirt(self):
        assert "ialirt" in self._handler().get_folder_structure()

    def test_get_full_path_combines_folder_and_filename(self):
        handler = self._handler()
        full = handler.get_full_path()
        assert full == Path("quicklook/ialirt/2025/06/imap_quicklook_ialirt_20250615.png")

    def test_from_filename_parses_valid_filename(self):
        handler = IALiRTQuicklookPathHandler.from_filename("imap_quicklook_ialirt_20250615.png")
        assert handler is not None
        assert handler.content_date == datetime(2025, 6, 15)

    def test_from_filename_returns_none_for_non_matching(self):
        handler = IALiRTQuicklookPathHandler.from_filename("imap_quicklook_hk_20250615.png")
        assert handler is None

    def test_supports_sequencing_returns_false(self):
        assert self._handler().supports_sequencing() is False

    def test_get_folder_structure_fails_without_content_date(self):
        with pytest.raises(ValueError):
            IALiRTQuicklookPathHandler().get_folder_structure()
