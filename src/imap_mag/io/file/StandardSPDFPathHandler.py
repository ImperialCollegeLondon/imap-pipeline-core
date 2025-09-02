import logging
import re
from dataclasses import dataclass
from datetime import datetime

from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler

logger = logging.getLogger(__name__)


@dataclass
class StandardSPDFPathHandler(VersionedPathHandler):
    """
    Path handler for standard SPDF files.
    See: https://imap-processing.readthedocs.io/en/latest/data-access/naming-conventions.html#data-product-science-file-naming-conventions
    """

    mission: str = "imap"
    instrument: str = "mag"
    level: str | None = None
    descriptor: str | None = None
    content_date: datetime | None = None  # date data belongs to
    extension: str | None = None

    def get_filename(self) -> str:
        super()._check_property_values(
            "file name", ["descriptor", "level", "content_date", "extension"]
        )
        assert self.content_date

        return f"{self.mission}_{self.instrument}_{self.level}_{self.descriptor}_{self.content_date.strftime('%Y%m%d')}_v{self.version:03}.{self.extension}"

    def get_date_for_indexing(self):
        return self.content_date

    def get_unsequenced_pattern(self) -> re.Pattern:
        super()._check_property_values(
            "pattern", ["descriptor", "level", "content_date", "extension"]
        )
        assert self.descriptor and self.content_date

        return re.compile(
            rf"{self.mission}_{self.instrument}_{self.level}_{re.escape(self.descriptor)}_{self.content_date.strftime('%Y%m%d')}_v(?P<version>\d+)\.{self.extension}"
        )
