import abc
from dataclasses import dataclass
from datetime import datetime


@dataclass
class SDCQueryParameters(abc.ABC):
    """Abstract base class for query parameters used in SDC data access."""

    @property
    @abc.abstractmethod
    def table(self) -> str:
        """Return the name of the table to query."""
        pass

    @abc.abstractmethod
    def to_dict(self) -> dict[str, str | None]:
        """Convert parameters to a dictionary suitable for querying SDC."""
        pass


@dataclass
class ScienceQueryParameters(SDCQueryParameters):
    """Query parameters for science data."""

    instrument: str = "mag"
    level: str | None = None
    descriptor: str | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    ingestion_start_date: datetime | None = None
    ingestion_end_date: datetime | None = None
    version: str | None = None
    extension: str | None = None

    @property
    def table(self) -> str:
        return "science"

    def to_dict(self) -> dict[str, str | None]:
        return {
            "data_level": self.level,
            "descriptor": self.descriptor,
            "start_date": (
                self.start_date.strftime("%Y%m%d") if self.start_date else None
            ),
            "end_date": self.end_date.strftime("%Y%m%d") if self.end_date else None,
            "ingestion_start_date": (
                self.ingestion_start_date.strftime("%Y%m%d")
                if self.ingestion_start_date
                else None
            ),
            "ingestion_end_date": (
                self.ingestion_end_date.strftime("%Y%m%d")
                if self.ingestion_end_date
                else None
            ),
            "version": self.version,
            "extension": self.extension,
        }


@dataclass
class SpiceQueryParameters(SDCQueryParameters):
    """Query parameters for SPICE data."""

    type: str | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    ingestion_start_date: datetime | None = None
    ingestion_end_date: datetime | None = None
    version: str | None = None

    @property
    def table(self) -> str:
        return "spice"

    def to_dict(self) -> dict[str, str | None]:
        return {
            # "kernel_type": self.type,
            "start_date": (
                self.start_date.strftime("%Y%m%d") if self.start_date else None
            ),
            "end_date": self.end_date.strftime("%Y%m%d") if self.end_date else None,
            "ingestion_start_date": (
                self.ingestion_start_date.strftime("%Y%m%d")
                if self.ingestion_start_date
                else None
            ),
            "ingestion_end_date": (
                self.ingestion_end_date.strftime("%Y%m%d")
                if self.ingestion_end_date
                else None
            ),
            "version": self.version,
        }
