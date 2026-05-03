import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.file.IFilePathHandler import IFilePathHandler

logger = logging.getLogger(__name__)


@dataclass
class IALiRTInstrumentPathHandler(IFilePathHandler):
    """
    Path handler for I-ALiRT instrument files (non-MAG, non-HK).

    Produces files of the form:
        ialirt/YYYY/MM/imap_ialirt_<instrument>_YYYYMMDD.csv
    """

    root_folder: str = "ialirt"

    mission: str = "imap"
    instrument: str = ""
    content_date: datetime | None = None
    extension: str = "csv"

    def supports_sequencing(self) -> bool:
        return False

    def get_content_date_for_indexing(self) -> datetime | None:
        return self.content_date

    def get_folder_structure(self) -> str:
        super()._check_property_values("folder structure", ["content_date"])
        assert self.content_date

        return (Path(self.root_folder) / self.content_date.strftime("%Y/%m")).as_posix()

    def get_filename(self) -> str:
        super()._check_property_values("file name", ["content_date", "instrument"])
        assert self.content_date
        assert self.instrument

        return f"{self.mission}_ialirt_{self.instrument}_{self.content_date.strftime('%Y%m%d')}.{self.extension}"

    def add_metadata(self, metadata: dict) -> None:
        raise NotImplementedError()

    def get_metadata(self) -> dict | None:
        return None

    @classmethod
    def from_filename(
        cls, filename: str | Path
    ) -> "IALiRTInstrumentPathHandler | None":
        match = re.match(
            r"imap_ialirt_(?P<instrument>[a-z][a-z0-9_]+)_(?P<date>\d{8})\.(?P<ext>\w+)",
            Path(filename).name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with instrument regex."
        )

        if match is None:
            return None
        else:
            return cls(
                instrument=match["instrument"],
                content_date=datetime.strptime(match["date"], "%Y%m%d"),
                extension=match["ext"],
            )
