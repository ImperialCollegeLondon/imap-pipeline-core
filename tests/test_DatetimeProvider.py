"""Tests for DatetimeProvider utility."""

from datetime import UTC, date, datetime

from imap_mag.util.DatetimeProvider import DatetimeProvider


class TestDatetimeProviderNow:
    def test_now_returns_datetime(self):
        result = DatetimeProvider.now()
        assert isinstance(result, datetime)

    def test_now_has_no_timezone(self):
        result = DatetimeProvider.now()
        assert result.tzinfo is None

    def test_now_is_recent(self):
        before = datetime.now(UTC).replace(tzinfo=None)
        result = DatetimeProvider.now()
        after = datetime.now(UTC).replace(tzinfo=None)
        assert before <= result <= after


class TestDatetimeProviderToday:
    def test_today_returns_datetime_by_default(self):
        result = DatetimeProvider.today()
        assert isinstance(result, datetime)

    def test_today_with_date_type_returns_date(self):
        result = DatetimeProvider.today(date)
        assert type(result) is date

    def test_today_datetime_is_midnight(self):
        result = DatetimeProvider.today()
        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0
        assert result.microsecond == 0

    def test_today_has_no_timezone(self):
        result = DatetimeProvider.today()
        assert result.tzinfo is None


class TestDatetimeProviderTomorrow:
    def test_tomorrow_is_one_day_after_today(self):
        today = DatetimeProvider.today()
        tomorrow = DatetimeProvider.tomorrow()
        assert (tomorrow - today).days == 1

    def test_tomorrow_with_date_type(self):
        result = DatetimeProvider.tomorrow(date)
        assert type(result) is date


class TestDatetimeProviderYesterday:
    def test_yesterday_is_one_day_before_today(self):
        today = DatetimeProvider.today()
        yesterday = DatetimeProvider.yesterday()
        assert (today - yesterday).days == 1

    def test_yesterday_with_date_type(self):
        result = DatetimeProvider.yesterday(date)
        assert type(result) is date


class TestDatetimeProviderStartAndEndOfHour:
    def test_start_of_hour_has_zero_minutes_and_seconds(self):
        result = DatetimeProvider.start_of_hour()
        assert result.minute == 0
        assert result.second == 0
        assert result.microsecond == 0

    def test_end_of_hour_has_59_minutes(self):
        result = DatetimeProvider.end_of_hour()
        assert result.minute == 59
        assert result.second == 59
        assert result.microsecond == 999999


class TestDatetimeProviderEndOfToday:
    def test_end_of_today_is_23_59_59(self):
        result = DatetimeProvider.end_of_today()
        assert result.hour == 23
        assert result.minute == 59
        assert result.second == 59
        assert result.microsecond == 999999


class TestDatetimeProviderBeginningOfImap:
    def test_returns_imap_launch_date(self):
        result = DatetimeProvider.beginning_of_imap()
        assert result.year == 2025
        assert result.month == 9
        assert result.day == 24

    def test_returns_date_type_when_requested(self):
        result = DatetimeProvider.beginning_of_imap(date)
        assert type(result) is date
        assert result.year == 2025
