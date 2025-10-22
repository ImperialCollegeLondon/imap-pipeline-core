import abc
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class SeverityLevel(Enum):
    """Enumeration of severity levels for I-ALiRT failures."""

    Danger = "danger"
    Warning = "warning"


@dataclass
class IALiRTFailure(abc.ABC):
    """Represents a failure found in I-ALiRT data."""

    time_range: tuple[datetime, datetime]
    parameter: str
    severity: SeverityLevel

    @abc.abstractmethod
    def __str__(self) -> str:
        pass

    def print(self) -> None:
        """Prints a summary of the failure."""

        if self.severity == SeverityLevel.Danger:
            logger.error(str(self))
        else:
            logger.warning(str(self))


@dataclass
class IALiRTOutOfBoundsFailure(IALiRTFailure):
    """Represents an out-of-bounds failure in I-ALiRT data."""

    values: tuple[float, float]
    limits: tuple[float, float]

    def __str__(self) -> str:
        return (
            f"[{self.severity.value.upper()}] {self.parameter} out of bounds "
            f"at least once from {self.time_range[0]} to {self.time_range[1]}: "
            f"values {self.values} outside limits {self.limits}."
        )


@dataclass
class IALiRTFlagFailure(IALiRTFailure):
    """Represents a flag failure in I-ALiRT data."""

    def __str__(self) -> str:
        return (
            f"[{self.severity.value.upper()}] {self.parameter} {self.severity.value.lower()} flag is high "
            f"at least once from {self.time_range[0]} to {self.time_range[1]}."
        )


@dataclass
class IALiRTForbiddenValueFailure(IALiRTFailure):
    """Represents a forbidden value failure in I-ALiRT data."""

    value: float | str

    def __str__(self) -> str:
        return (
            f"[{self.severity.value.upper()}] {self.parameter} has forbidden value {self.value} "
            f"at least once from {self.time_range[0]} to {self.time_range[1]}."
        )
