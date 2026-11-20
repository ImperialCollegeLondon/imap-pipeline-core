"""Tests for LeinweberOffsetsPathHandler."""

from datetime import datetime

import pytest

from imap_mag.io.file.LeinweberOffsetsPathHandler import (
    LEINWEBER,
    LeinweberOffsetsPathHandler,
)

DATE = datetime(2026, 1, 14)


def _handler(
    sensor: str = "mago",
    version: int = 24,
    duration_hours: int | None = None,
) -> LeinweberOffsetsPathHandler:
    return LeinweberOffsetsPathHandler(
        sensor=sensor,
        content_date=DATE,
        version=version,
        duration_hours=duration_hours,
    )


class TestLeinweberOffsetsPathHandler:
    def test_offset_type_defaults_to_leinweber(self):
        assert _handler().offset_type == LEINWEBER

    def test_folder_structure(self):
        assert (
            _handler(duration_hours=24).get_folder_structure()
            == "calibration/calculated_offsets/leinweber"
        )

    def test_filename(self):
        assert (
            _handler(sensor="magi", duration_hours=24, version=3).get_filename()
            == "imap_mag_magi-raw-leinweber-offsets-24hr_20260114_v003.csv"
        )

    def test_filename_requires_duration_hours(self):
        with pytest.raises(ValueError):
            _handler().get_filename()

    def test_from_filename_roundtrip(self):
        handler = LeinweberOffsetsPathHandler.from_filename(
            "calculated_offsets/leinweber/"
            "imap_mag_mago-raw-leinweber-offsets-24hr_20260114_v007.csv"
        )
        assert handler is not None
        assert handler.sensor == "mago"
        assert handler.offset_type == LEINWEBER
        assert handler.duration_hours == 24
        assert handler.content_date == DATE
        assert handler.version == 7

    def test_from_filename_rejects_folder_mismatch(self, tmp_path):
        f = (
            tmp_path
            / "spin_plane"
            / "imap_mag_mago-raw-leinweber-offsets-24hr_20260114_v000.csv"
        )
        f.parent.mkdir(parents=True)
        f.write_text("data")
        with pytest.raises(ValueError, match="implies offset type"):
            LeinweberOffsetsPathHandler.from_filename(f)

    def test_from_filename_rejects_unrelated_names(self):
        assert LeinweberOffsetsPathHandler.from_filename("something_else.csv") is None
        assert (
            LeinweberOffsetsPathHandler.from_filename(
                "imap_mag_mago-spin-plane-offsets_20260114_v001.csv"
            )
            is None
        )

    def test_from_filename_file(self, tmp_path):
        f = (
            tmp_path
            / LEINWEBER
            / "imap_mag_mago-raw-leinweber-offsets-24hr_20260114_v000.csv"
        )
        f.parent.mkdir(parents=True)
        f.write_text("data")

        handler = LeinweberOffsetsPathHandler.from_filename(f)
        assert handler is not None
        assert handler.sensor == "mago"
        assert handler.duration_hours == 24
        assert handler.content_date == DATE
        assert (
            handler.get_folder_structure() == "calibration/calculated_offsets/leinweber"
        )
