"""Tests for CalculatedOffsetsPathHandler."""

from datetime import datetime
from pathlib import Path

from imap_mag.io.file.CalculatedOffsetsPathHandler import (
    SPIN_OPTIMISED,
    SPIN_PLANE,
    CalculatedOffsetsPathHandler,
)

DATE = datetime(2026, 1, 14)


def _handler(
    sensor: str = "mago",
    offset_type: str = SPIN_PLANE,
    version: int = 24,
) -> CalculatedOffsetsPathHandler:
    return CalculatedOffsetsPathHandler(
        sensor=sensor,
        offset_type=offset_type,
        content_date=DATE,
        version=version,
    )


class TestCalculatedOffsetsPathHandler:
    def test_folder_structure_per_offset_type(self):
        assert (
            _handler(offset_type=SPIN_PLANE).get_folder_structure()
            == "calibration/calculated_offsets/spin_plane"
        )
        assert (
            _handler(offset_type=SPIN_OPTIMISED).get_folder_structure()
            == "calibration/calculated_offsets/spin_optimised"
        )

    def test_filename(self):
        assert (
            _handler(sensor="magi", version=3).get_filename()
            == "imap_mag_magi-spin-plane-offsets_20260114_v003.csv"
        )

    def test_full_path(self):
        assert _handler(version=24).get_full_path(Path("/store")) == Path(
            "/store/calibration/calculated_offsets/spin_plane/"
            "imap_mag_mago-spin-plane-offsets_20260114_v024.csv"
        )

    def test_filename_and_folder_identical_across_offset_types(self):
        # Both folders hold identically-named files; only the folder differs.
        plane = _handler(offset_type=SPIN_PLANE)
        optimised = _handler(offset_type=SPIN_OPTIMISED)
        assert plane.get_filename() == optimised.get_filename()
        assert plane.get_folder_structure() != optimised.get_folder_structure()

    def test_supports_sequencing_and_bumps_version(self):
        handler = _handler(version=24)
        assert handler.supports_sequencing() is True
        handler.increase_sequence()
        assert handler.version == 25
        assert "_v025.csv" in handler.get_filename()

    def test_unsequenced_pattern_matches_all_versions(self):
        pattern = _handler().get_unsequenced_pattern()
        m = pattern.search("imap_mag_mago-spin-plane-offsets_20260114_v024.csv")
        assert m is not None
        assert m.group("version") == "024"
        # Different sensor / date must not match.
        assert (
            pattern.search("imap_mag_magi-spin-plane-offsets_20260114_v024.csv") is None
        )
        assert (
            pattern.search("imap_mag_mago-spin-plane-offsets_20260115_v024.csv") is None
        )

    def test_from_filename_roundtrip(self):
        handler = CalculatedOffsetsPathHandler.from_filename(
            f"{SPIN_PLANE}/imap_mag_magi-spin-plane-offsets_20260114_v007.csv"
        )
        assert handler is not None
        assert handler.sensor == "magi"
        assert handler.content_date == DATE
        assert handler.version == 7
        # offset_type is unknowable from the name alone.
        assert handler.offset_type == SPIN_PLANE

    def test_from_filename_rejects_unrelated_names(self):
        assert CalculatedOffsetsPathHandler.from_filename("something_else.csv") is None
        assert (
            CalculatedOffsetsPathHandler.from_filename(
                "imap_mag_l2-norm-offsets_20260114_v001.cdf"
            )
            is None
        )

    def test_from_filename_infers_offset_type_from_parent(self, tmp_path):
        f: Path = (
            tmp_path
            / SPIN_OPTIMISED
            / "imap_mag_mago-spin-plane-offsets_20260114_v000.csv"
        )
        f.parent.mkdir(parents=True)
        f.write_text("data")

        handler = CalculatedOffsetsPathHandler.from_filename(f)
        assert handler is not None
        assert handler.offset_type == SPIN_OPTIMISED
        assert handler.sensor == "mago"
        assert handler.content_date == DATE
        assert handler.get_folder_structure() == (
            "calibration/calculated_offsets/spin_optimised"
        )

    def test_from_filename_rejects_bad_folder(self, tmp_path):
        f = tmp_path / "wrong" / "imap_mag_mago-spin-plane-offsets_20260114_v000.csv"
        f.parent.mkdir(parents=True)
        f.write_text("data")
        handler = CalculatedOffsetsPathHandler.from_filename(f)
        assert handler is None

    def test_from_filename_rejects_bad_name(self, tmp_path):
        f = tmp_path / SPIN_PLANE / "not-an-offsets-file.csv"
        f.parent.mkdir(parents=True)
        f.write_text("data")
        handler = CalculatedOffsetsPathHandler.from_filename(f)
        assert handler is None
