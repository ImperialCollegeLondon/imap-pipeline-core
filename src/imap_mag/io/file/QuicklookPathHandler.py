import abc
import logging
import re
import typing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.file.IFilePathHandler import IFilePathHandler

logger = logging.getLogger(__name__)

T = typing.TypeVar("T", bound="QuicklookPathHandler")


@dataclass
class QuicklookPathHandler(IFilePathHandler):
    """
    Path handler for figures.
    """

    root_folder: str = "quicklook"

    mission: str = "imap"
    start_date: datetime | None = None
    end_date: datetime | None = None
    extension: str = "png"

    @property
    @abc.abstractmethod
    def plot_type(self) -> str:
        """The type of plot (e.g., "ialirt", "hk", etc.)."""
        pass

    def supports_sequencing(self) -> bool:
        return False

    def get_content_date_for_indexing(self) -> datetime | None:
        return self.start_date

    def get_folder_structure(self) -> str:
        super()._check_property_values("folder structure", ["start_date"])
        assert self.start_date

        return (
            Path(self.root_folder) / self.plot_type / self.start_date.strftime("%Y/%m")
        ).as_posix()

    def get_filename(self) -> str:
        super()._check_property_values("file name", ["start_date", "end_date"])
        assert self.start_date and self.end_date

        return f"{self.mission}_quicklook_{self.plot_type}_{self.start_date.strftime('%Y%m%d')}_{self.end_date.strftime('%Y%m%d')}.{self.extension}"

    @classmethod
    def from_filename(cls: type[T], filename: str | Path) -> T | None:
        dummy = cls()
        match = re.match(
            rf"imap_quicklook_{dummy.plot_type}_(?P<start_date>\d{{8}})_(?P<end_date>\d{{8}})\.(?P<ext>\w+)",
            Path(filename).name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with quicklook regex."
        )

        if match is None:
            return None
        else:
            return cls(
                start_date=datetime.strptime(match["start_date"], "%Y%m%d"),
                end_date=datetime.strptime(match["end_date"], "%Y%m%d"),
                extension=match["ext"],
            )
