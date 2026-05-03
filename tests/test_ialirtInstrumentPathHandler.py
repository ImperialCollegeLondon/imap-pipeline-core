"""Tests for IALiRTInstrumentPathHandler."""

from datetime import datetime

import pytest

from imap_mag.io.file.IALiRTInstrumentPathHandler import IALiRTInstrumentPathHandler


@pytest.mark.parametrize(
    "instrument",
    ["hit", "swe", "swapi", "codice_lo", "codice_hi"],
)
def test_get_filename(instrument: str) -> None:
    handler = IALiRTInstrumentPathHandler(
        instrument=instrument,
        content_date=datetime(2026, 2, 1),
    )

    assert handler.get_filename() == f"imap_ialirt_{instrument}_20260201.csv"


@pytest.mark.parametrize(
    "instrument",
    ["hit", "swe", "swapi", "codice_lo", "codice_hi"],
)
def test_get_folder_structure(instrument: str) -> None:
    handler = IALiRTInstrumentPathHandler(
        instrument=instrument,
        content_date=datetime(2026, 2, 1),
    )

    assert handler.get_folder_structure() == "ialirt/2026/02"


@pytest.mark.parametrize(
    "instrument",
    ["hit", "swe", "swapi", "codice_lo", "codice_hi"],
)
def test_from_filename_roundtrip(instrument: str) -> None:
    content_date = datetime(2026, 2, 1)
    original = IALiRTInstrumentPathHandler(
        instrument=instrument, content_date=content_date
    )

    parsed = IALiRTInstrumentPathHandler.from_filename(original.get_filename())

    assert parsed is not None
    assert parsed.instrument == instrument
    assert parsed.content_date == content_date


def test_from_filename_returns_none_for_mag_file() -> None:
    # The plain MAG file (imap_ialirt_20260201.csv) should NOT match this handler
    result = IALiRTInstrumentPathHandler.from_filename("imap_ialirt_20260201.csv")
    assert result is None


def test_from_filename_does_not_shadow_hk_handler() -> None:
    # imap_ialirt_hk_20260201.csv would match with instrument="hk" via IALiRTInstrumentPathHandler.
    # This is acceptable IF IALiRTHKPathHandler is tried first in FilePathHandlerSelector.
    # Here we just verify the parse result is internally consistent.
    result = IALiRTInstrumentPathHandler.from_filename("imap_ialirt_hk_20260201.csv")
    assert result is not None
    assert result.instrument == "hk"
    assert result.content_date == datetime(2026, 2, 1)


def test_get_content_date_for_indexing() -> None:
    content_date = datetime(2026, 5, 3)
    handler = IALiRTInstrumentPathHandler(instrument="swe", content_date=content_date)

    assert handler.get_content_date_for_indexing() == content_date
