from datetime import datetime

import pytest
import typer

from imap_mag.cli.fetch.ialirt import fetch_ialirt


def test_fetch_ialirt_invalid_instrument_raises_bad_parameter():
    """
    Verify that passing an unsupported instrument string stops
    execution and raises a typer.BadParameter exception.
    """
    dummy_start = datetime(2025, 1, 2, 0, 0)
    dummy_end = datetime(2025, 1, 3, 0, 0)
    invalid_instrument = "not_a_real_instrument"

    with pytest.raises(typer.BadParameter) as exc_info:
        fetch_ialirt(
            start_date=dummy_start,
            end_date=dummy_end,
            instrument=invalid_instrument,
            fetch_mode=typer.Option(
                case_sensitive=False,
            ),
        )

    assert "is not a valid instrument" in str(exc_info.value)
