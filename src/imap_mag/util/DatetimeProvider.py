import typing
from datetime import date, datetime, timedelta

D = typing.TypeVar("D", date, datetime)


class DatetimeProvider:
    """Datetime provider to remove dependency on `datetime` library."""

    @staticmethod
    def now() -> datetime:
        return datetime.now()

    @staticmethod
    def today(date_type: type[D] = datetime) -> D:
        today = date_type.today()

        if isinstance(today, datetime):
            return today.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            return today

    @classmethod
    def tomorrow(cls, date_type: type[D] = datetime) -> D:
        return cls.today(date_type) + timedelta(days=1)

    @classmethod
    def yesterday(cls, date_type: type[D] = datetime) -> D:
        return cls.today(date_type) - timedelta(days=1)

    @staticmethod
    def end_of_today() -> datetime:
        return datetime.today().replace(
            hour=23, minute=59, second=59, microsecond=999999
        )

    @classmethod
    def beginning_of_imap(cls, date_type: type[D] = datetime) -> D:
        return cls.today(date_type).replace(year=2025, month=1, day=1)
