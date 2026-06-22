import typing
from datetime import UTC, date, datetime, timedelta

T = typing.TypeVar("T", date, datetime)


class DatetimeProvider:
    """Datetime provider to remove dependency on `datetime` library.

    Pass ``fixed_now`` to pin all time-dependent methods to a specific instant
    (useful in tests to avoid patching shared class state).
    """

    def __init__(self, fixed_now: datetime | None = None) -> None:
        self._fixed_now = fixed_now
        self._mission_start_date = datetime(2025, 9, 24, 0, 0, 0)

    def _get_now(self) -> datetime:
        if self._fixed_now is not None:
            return self._fixed_now
        return datetime.now(UTC).replace(tzinfo=None)

    def now(self) -> datetime:
        return self._get_now()

    def today(self, date_type: type[T] = datetime) -> T:
        now = self._get_now()
        if date_type is datetime or issubclass(date_type, datetime):
            return now.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            return now.date()

    def tomorrow(self, date_type: type[T] = datetime) -> T:
        return self.today(date_type) + timedelta(days=1)

    def yesterday(self, date_type: type[T] = datetime) -> T:
        return self.today(date_type) - timedelta(days=1)

    def start_of_hour(self) -> datetime:
        return self.now().replace(minute=0, second=0, microsecond=0)

    def end_of_hour(self) -> datetime:
        return self.now().replace(minute=59, second=59, microsecond=999999)

    def end_of_today(self) -> datetime:
        return self.today().replace(hour=23, minute=59, second=59, microsecond=999999)

    def beginning_of_imap(self) -> datetime:
        return self._mission_start_date
