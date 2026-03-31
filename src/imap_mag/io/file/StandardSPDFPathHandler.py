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
        assert self.extension

        return self.generate_filename_from_logical_source(
            logical_source=f"{self.mission}_{self.instrument}_{self.level}_{self.descriptor}",
            content_date=self.content_date,
            version=self.version,
            extension=self.extension,
        )

    def get_content_date_for_indexing(self):
        return self.content_date

    def get_unsequenced_pattern(self) -> re.Pattern:
        super()._check_property_values(
            "pattern", ["descriptor", "level", "content_date", "extension"]
        )
        assert self.descriptor and self.content_date

        return re.compile(
            rf"{self.mission}_{self.instrument}_{self.level}_{re.escape(self.descriptor)}_{self.content_date.strftime('%Y%m%d')}_v(?P<version>\d+)\.{self.extension}"
        )

    @classmethod
    def generate_filename_from_logical_source(
        cls, logical_source: str, content_date: datetime, version: int, extension: str
    ) -> str:
        if not logical_source:
            raise ValueError("Logical_source is required to generate filename")
        if not content_date:
            raise ValueError("content_date is required to generate filename")
        if version is None:
            raise ValueError("version is required to generate filename")
        if not extension:
            raise ValueError("extension is required to generate filename")

        return f"{logical_source.lower()}_{content_date.strftime('%Y%m%d')}_v{version:03d}.{extension}"
