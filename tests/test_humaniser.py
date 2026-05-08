"""Tests for the Humaniser utility class."""

import pytest

from imap_mag.util.Humaniser import Humaniser


@pytest.mark.parametrize(
    "num_bytes,expected",
    [
        (0, "0.0B"),
        (1023, "1023.0B"),
        (1024, "1.0KiB"),
        (1024 * 1024, "1.0MiB"),
        (1024 * 1024 * 1024, "1.0GiB"),
        (1024**4, "1.0TiB"),
        (1536, "1.5KiB"),
    ],
)
def test_format_bytes_standard_sizes(num_bytes, expected):
    assert Humaniser.format_bytes(num_bytes) == expected


def test_format_bytes_uses_custom_suffix():
    result = Humaniser.format_bytes(1024, suffix="bytes")
    assert result == "1.0Kibytes"


def test_format_bytes_handles_negative_values():
    result = Humaniser.format_bytes(-1024)
    assert "KiB" in result
