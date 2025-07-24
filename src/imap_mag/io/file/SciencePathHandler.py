import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.file.StandardSPDFPathHandler import StandardSPDFPathHandler

logger = logging.getLogger(__name__)


@dataclass
class SciencePathHandler(StandardSPDFPathHandler):
    """
    Path handler for science files.
    """

    root_folder: str = "science"
    ingestion_date: datetime | None = None  # date data was ingested by SDC

    def get_folder_structure(self) -> str:
        super()._check_property_values("folder structure", ["content_date", "level"])
        assert self.content_date and self.level

        return (
            Path(self.root_folder)
            / self.instrument
            / self.level
            / self.content_date.strftime("%Y/%m")
        ).as_posix()

    @classmethod
    def from_filename(cls, filename: str | Path) -> "SciencePathHandler | None":
        match = re.match(
            r"imap_mag_(?P<level>l\d[a-zA-Z]?(?:-pre)?)_(?P<descr>(?:norm|burst)[^_]*)_(?P<date>\d{8})_v(?P<version>\d+)\.(?P<ext>\w+)",
            Path(filename).name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with science regex."
        )

        if match is None:
            return None
        else:
            return cls(
                level=match["level"],
                descriptor=match["descr"],
                content_date=datetime.strptime(match["date"], "%Y%m%d"),
                version=int(match["version"]),
                extension=match["ext"],
            )
