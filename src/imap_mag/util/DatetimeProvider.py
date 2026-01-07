import typing
from datetime import UTC, date, datetime, timedelta

T = typing.TypeVar("T", date, datetime)


class DatetimeProvider:
    """Datetime provider to remove dependency on `datetime` library."""

    @staticmethod
    def now() -> datetime:
        return datetime.now(UTC).replace(tzinfo=None)

    @staticmethod
    def today(date_type: type[T] = datetime) -> T:
        today = date_type.today()

        if isinstance(today, datetime):
            return today.astimezone(UTC).replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=None
            )
        else:
            return today

    @classmethod
    def tomorrow(cls, date_type: type[T] = datetime) -> T:
        return cls.today(date_type) + timedelta(days=1)

    @classmethod
    def yesterday(cls, date_type: type[T] = datetime) -> T:
        return cls.today(date_type) - timedelta(days=1)

    @classmethod
    def start_of_hour(cls) -> datetime:
        return cls.now().replace(minute=0, second=0, microsecond=0)

    @classmethod
    def end_of_hour(cls) -> datetime:
        return cls.now().replace(minute=59, second=59, microsecond=999999)

    @classmethod
    def end_of_today(cls) -> datetime:
        return cls.today().replace(hour=23, minute=59, second=59, microsecond=999999)

    @classmethod
    def beginning_of_imap(cls, date_type: type[T] = datetime) -> T:
        return cls.today(date_type).replace(year=2025, month=9, day=24)
