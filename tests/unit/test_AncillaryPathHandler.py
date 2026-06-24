"""Tests for AncillaryPathHandler."""

from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.io.file.AncillaryPathHandler import AncillaryPathHandler


def _offsets_handler(start=None, end=None, version=1) -> AncillaryPathHandler:
    return AncillaryPathHandler(
        descriptor="l2-norm-offsets",
        start_date=start or datetime(2025, 10, 17),
        end_date=end,
        version=version,
        extension="cdf",
    )


class TestAncillaryPathHandler:
    def test_get_folder_structure_for_offsets_file(self):
        folder = _offsets_handler().get_folder_structure()
        assert "science-ancillary" in folder
        assert "l2-offsets" in folder

    def test_get_filename_with_end_date(self):
        handler = _offsets_handler(
            start=datetime(2025, 10, 17),
            end=datetime(2025, 10, 20),
            version=2,
        )
        assert (
            handler.get_filename()
            == "imap_mag_l2-norm-offsets_20251017_20251020_v002.cdf"
        )

    def test_get_filename_without_end_date(self):
        handler = _offsets_handler(start=datetime(2025, 10, 17))
        assert handler.get_filename() == "imap_mag_l2-norm-offsets_20251017_v001.cdf"

    def test_get_content_date_for_indexing_returns_start_date_for_offsets(self):
        dt = datetime(2025, 10, 17)
        handler = _offsets_handler(start=dt)
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
        handler = _offsets_handler(
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
        handler = AncillaryPathHandler.from_filename(
            "imap_mag_l2_norm-mago_20251017_v001.cdf"
        )
        assert handler is None
