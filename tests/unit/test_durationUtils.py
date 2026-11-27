from datetime import timedelta

import pytest

from prefect_server.durationUtils import format_duration, parse_duration, to_timedelta


def test_parses_days():
    assert parse_duration("30d") == timedelta(days=30)
    assert parse_duration("1D") == timedelta(days=1)


def test_parses_hours():
    assert parse_duration("12h") == timedelta(hours=12)
    assert parse_duration("24H") == timedelta(hours=24)


def test_parses_minutes():
    assert parse_duration("45m") == timedelta(minutes=45)
    assert parse_duration("60M") == timedelta(minutes=60)


def test_parses_seconds():
    assert parse_duration("30s") == timedelta(seconds=30)
    assert parse_duration("60S") == timedelta(seconds=60)


def test_parses_combinations():
    assert parse_duration("1d12h") == timedelta(days=1, hours=12)
    assert parse_duration("2d6h30m") == timedelta(days=2, hours=6, minutes=30)


def test_raises_on_invalid_format():
    with pytest.raises(ValueError):
        parse_duration("invalid")
    with pytest.raises(ValueError):
        parse_duration("")
    with pytest.raises(ValueError):
        parse_duration("30")


def test_formats_days():
    assert format_duration(timedelta(days=30)) == "30d"


def test_formats_hours():
    assert format_duration(timedelta(hours=12)) == "12h"


def test_formats_combinations():
    assert format_duration(timedelta(days=1, hours=12)) == "1d12h"


def test_formats_zero():
    assert format_duration(timedelta(0)) == "0s"


def test_to_timedelta_accepts_timedeltas_seconds_and_duration_strings():
    assert to_timedelta(timedelta(hours=2)) == timedelta(hours=2)
    assert to_timedelta(90) == timedelta(seconds=90)
    assert to_timedelta(1.5) == timedelta(seconds=1.5)
    assert to_timedelta("5m") == timedelta(minutes=5)
    with pytest.raises(ValueError):
        to_timedelta("soon")
