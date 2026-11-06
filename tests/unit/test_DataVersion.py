"""Tests for the format_data_version util."""

import pytest

from imap_mag.util import get_cdf_data_version


@pytest.mark.parametrize(
    ("version", "version_major", "expected"),
    [
        (1, 0, "001"),
        (12, 0, "012"),
        (2, 1, "001.0002"),
        (345, 12, "012.0345"),
    ],
)
def test_format_data_version(version, version_major, expected):
    assert get_cdf_data_version(version, version_major) == expected


def test_major_version_defaults_to_zero():
    assert get_cdf_data_version(7) == "007"
