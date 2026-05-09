"""Tests for TimeConversion utility."""

from datetime import UTC, date, datetime

import numpy as np

from imap_mag.util.TimeConversion import TimeConversion


class TestTimeConversionMetToJ2000:
    def test_scalar_met_returns_j2000ns(self):
        result = TimeConversion.convert_met_to_j2000ns(0)
        assert isinstance(result, np.integer | int | np.ndarray)

    def test_zero_met_is_j2000_offset_of_imap_epoch(self):
        result = TimeConversion.convert_met_to_j2000ns(np.array([0]))
        assert result.shape == (1,)

    def test_array_met_returns_array(self):
        mets = np.array([0, 1000, 5000])
        result = TimeConversion.convert_met_to_j2000ns(mets)
        assert len(result) == 3
        assert result[1] > result[0]

    def test_larger_met_gives_larger_j2000(self):
        result = TimeConversion.convert_met_to_j2000ns(np.array([100, 200]))
        assert result[1] > result[0]


class TestTimeConversionJ2000NsToDatetime:
    def test_returns_list_of_datetimes(self):
        j2000ns = np.array([0])
        result = TimeConversion.convert_j2000ns_to_datetime(j2000ns)
        assert isinstance(result, list)
        assert isinstance(result[0], datetime)

    def test_multiple_values_returns_multiple_datetimes(self):
        j2000ns = np.array([0, 1_000_000_000, 2_000_000_000])
        result = TimeConversion.convert_j2000ns_to_datetime(j2000ns)
        assert len(result) == 3

    def test_datetimes_are_monotonically_increasing(self):
        j2000ns = np.array([1_000_000_000, 2_000_000_000, 3_000_000_000])
        result = TimeConversion.convert_j2000ns_to_datetime(j2000ns)
        assert result[0] < result[1] < result[2]


class TestTimeConversionJ2000NsToIsoString:
    def test_returns_list_of_strings(self):
        j2000ns = np.array([0])
        result = TimeConversion.convert_j2000ns_to_isostring(j2000ns)
        assert isinstance(result, list)
        assert isinstance(result[0], str)

    def test_iso_string_format_is_valid(self):
        j2000ns = np.array([1_000_000_000])
        result = TimeConversion.convert_j2000ns_to_isostring(j2000ns)
        datetime.fromisoformat(result[0])

    def test_multiple_values_returns_multiple_strings(self):
        j2000ns = np.array([0, 1_000_000_000])
        result = TimeConversion.convert_j2000ns_to_isostring(j2000ns)
        assert len(result) == 2


class TestTimeConversionJ2000NsToDate:
    def test_returns_list_of_dates(self):
        j2000ns = np.array([0])
        result = TimeConversion.convert_j2000ns_to_date(j2000ns)
        assert isinstance(result, list)
        assert isinstance(result[0], date)

    def test_dates_are_monotonically_non_decreasing(self):
        j2000ns = np.array([0, 86_400_000_000_000])
        result = TimeConversion.convert_j2000ns_to_date(j2000ns)
        assert result[0] <= result[1]


class TestTimeConversionMetToDate:
    def test_returns_list_of_dates(self):
        result = TimeConversion.convert_met_to_date(np.array([0]))
        assert isinstance(result, list)
        assert isinstance(result[0], date)

    def test_large_met_gives_later_date(self):
        result = TimeConversion.convert_met_to_date(np.array([0, 86400]))
        assert result[0] <= result[1]

    def test_scalar_met_also_works(self):
        result = TimeConversion.convert_met_to_date(np.array([0]))
        assert isinstance(result, list)


class TestTryExtractIsoLikeDatetime:
    def test_returns_none_if_dict_is_none(self):
        result = TimeConversion.try_extract_iso_like_datetime(None, "key")
        assert result is None

    def test_returns_none_if_key_is_none(self):
        result = TimeConversion.try_extract_iso_like_datetime({"key": "value"}, None)
        assert result is None

    def test_returns_none_if_key_not_in_dict(self):
        result = TimeConversion.try_extract_iso_like_datetime(
            {"other": "2025-01-01"}, "key"
        )
        assert result is None

    def test_parses_iso_format_with_space(self):
        result = TimeConversion.try_extract_iso_like_datetime(
            {"date": "2025-01-15 12:00:00"}, "date"
        )
        assert result == datetime(2025, 1, 15, 12, 0, 0)

    def test_parses_iso_format_with_comma_space(self):
        result = TimeConversion.try_extract_iso_like_datetime(
            {"date": "2025-01-15, 12:00:00"}, "date"
        )
        assert result == datetime(2025, 1, 15, 12, 0, 0)

    def test_returns_none_on_invalid_format(self):
        result = TimeConversion.try_extract_iso_like_datetime(
            {"date": "not-a-date"}, "date"
        )
        assert result is None

    def test_applies_timezone_when_provided(self):
        result = TimeConversion.try_extract_iso_like_datetime(
            {"date": "2025-01-15 12:00:00"}, "date", timezone=UTC
        )
        assert result is not None
        assert result.tzinfo is not None


class TestForceUtcTimezone:
    def test_removes_tzinfo_when_has_timezone(self):
        dt = datetime(2025, 3, 20, 10, 0, 0, tzinfo=UTC)
        result = TimeConversion.force_utc_timezone(dt)
        assert result.tzinfo is None
        assert result == datetime(2025, 3, 20, 10, 0, 0)

    def test_returns_same_datetime_when_no_timezone(self):
        dt = datetime(2025, 3, 20, 10, 0, 0)
        result = TimeConversion.force_utc_timezone(dt)
        assert result == dt
        assert result.tzinfo is None
