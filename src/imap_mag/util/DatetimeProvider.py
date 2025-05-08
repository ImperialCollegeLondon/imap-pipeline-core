from datetime import datetime, timedelta


class DatetimeProvider:
    """Datetime provider to remove dependency on `datetime` library."""

    @staticmethod
    def now() -> datetime:
        return datetime.now()

    @staticmethod
    def today(date_type=datetime):
        today = date_type.today()

        if isinstance(today, datetime):
            return today.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            return today

    @classmethod
    def tomorrow(cls, date_type=datetime):
        return cls.today(date_type) + timedelta(days=1)

    @classmethod
    def yesterday(cls, date_type=datetime):
        return cls.today(date_type) - timedelta(days=1)

    @staticmethod
    def end_of_today() -> datetime:
        return datetime.today().replace(
            hour=23, minute=59, second=59, microsecond=999999
        )
