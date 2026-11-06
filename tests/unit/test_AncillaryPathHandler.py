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


def _write_offsets_cdf(folder: Path, filename: str) -> Path:
    from mag_toolkit.calibration import CalibrationLayer
    from tests.util.miscellaneous import write_calibration_layer_pair

    json_path, _ = write_calibration_layer_pair(
        folder, "manual-norm", datetime(2025, 10, 17), 1
    )
    layer = CalibrationLayer.from_file(json_path, load_contents=True)
    return layer.write_to_file(folder / filename)


def _read_cdf_global_attrs(cdf_file: Path) -> dict[str, str]:
    from tests.util.miscellaneous import open_cdf

    with open_cdf(cdf_file) as cdf:
        return {
            name: str(cdf.attrs[name][0]) if len(cdf.attrs[name]) else ""
            for name in cdf.attrs
        }


class TestPrepareForVersion:
    def test_stamps_data_version_and_logical_file_id_on_offsets_cdf(self, tmp_path):
        source = _write_offsets_cdf(
            tmp_path, "imap_mag_l2-norm-offsets_20251017_20251017_v001.cdf"
        )
        handler = _offsets_handler(
            start=datetime(2025, 10, 17), end=datetime(2025, 10, 17), version=7
        )

        stamped = handler.prepare_for_version(source)

        attrs = _read_cdf_global_attrs(stamped)
        assert attrs["Data_version"] == "007"
        assert (
            attrs["Logical_file_id"]
            == "imap_mag_l2-norm-offsets_20251017_20251017_v007"
        )

    def test_uses_major_version_when_set(self, tmp_path):
        source = _write_offsets_cdf(
            tmp_path, "imap_mag_l2-norm-offsets_20251017_20251017_v001.cdf"
        )
        handler = _offsets_handler(version=3)
        handler.version_major = 2

        stamped = handler.prepare_for_version(source)

        assert _read_cdf_global_attrs(stamped)["Data_version"] == "002.0003"

    def test_returns_a_copy_and_leaves_source_untouched(self, tmp_path):
        source = _write_offsets_cdf(
            tmp_path, "imap_mag_l2-norm-offsets_20251017_20251017_v001.cdf"
        )
        before = _read_cdf_global_attrs(source)

        stamped = _offsets_handler(version=5).prepare_for_version(source)

        assert stamped != source
        assert stamped.exists()
        assert _read_cdf_global_attrs(source) == before

    def test_returns_source_unchanged_for_non_offsets_ancillary_file(self, tmp_path):
        source = tmp_path / "imap_mag_l2-calibration_20251017_v001.cdf"
        source.write_bytes(b"not touched")
        handler = AncillaryPathHandler(
            descriptor="l2-calibration",
            start_date=datetime(2025, 10, 17),
            version=2,
            extension="cdf",
        )

        assert handler.prepare_for_version(source) == source

    def test_returns_source_unchanged_for_non_cdf_offsets_file(self, tmp_path):
        source = tmp_path / "imap_mag_l2-norm-offsets_20251017_v001.csv"
        source.write_text("a,b")
        handler = _offsets_handler(version=2)
        handler.extension = "csv"

        assert handler.prepare_for_version(source) == source
