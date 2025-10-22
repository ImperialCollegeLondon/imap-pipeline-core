import abc
import logging
from dataclasses import dataclass
from datetime import datetime

from imap_mag.check.SeverityLevel import SeverityLevel

logger = logging.getLogger(__name__)


@dataclass
class IALiRTAnomaly(abc.ABC):
    """Represents an anomaly found in I-ALiRT data."""

    time_range: tuple[datetime, datetime]
    parameter: str
    severity: SeverityLevel
    count: int

    @abc.abstractmethod
    def get_anomaly_description(self) -> str:
        pass

    def log(self) -> None:
        """Logs a summary of the anomaly."""

        if self.severity == SeverityLevel.Danger:
            logger.error(self.get_anomaly_description())
        else:
            logger.warning(self.get_anomaly_description())


@dataclass
class IALiRTOutOfBoundsAnomaly(IALiRTAnomaly):
    """Represents an out-of-bounds anomaly in I-ALiRT data."""

    value: float
    limits: tuple[float, float]

    def get_anomaly_description(self) -> str:
        return (
            f"[{self.severity.value.upper()}] {self.parameter} out of bounds "
            f"{self.count} time(s) from {self.time_range[0]} to {self.time_range[1]}: "
            f"value {self.value} outside {self.severity.value.lower()} limits {self.limits}."
        )


@dataclass
class IALiRTFlagAnomaly(IALiRTAnomaly):
    """Represents a flag anomaly in I-ALiRT data."""

    def get_anomaly_description(self) -> str:
        return (
            f"[{self.severity.value.upper()}] {self.parameter} {self.severity.value.lower()} flag is high "
            f"{self.count} time(s) from {self.time_range[0]} to {self.time_range[1]}."
        )


@dataclass
class IALiRTForbiddenValueAnomaly(IALiRTAnomaly):
    """Represents a forbidden value anomaly in I-ALiRT data."""

    value: float | str

    def get_anomaly_description(self) -> str:
        return (
            f"[{self.severity.value.upper()}] {self.parameter} has forbidden value {self.value} "
            f"{self.count} time(s) from {self.time_range[0]} to {self.time_range[1]}."
        )
