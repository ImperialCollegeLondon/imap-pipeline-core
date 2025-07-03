import logging
import re
from dataclasses import dataclass
from datetime import datetime

from imap_mag.io.IFileMetadataProvider import IFileMetadataProvider

logger = logging.getLogger(__name__)


@dataclass
class StandardSPDFMetadataProvider(IFileMetadataProvider):
    """
    Metadata for standard SPDF files.
    See: https://imap-processing.readthedocs.io/en/latest/data-access/naming-conventions.html#data-product-science-file-naming-conventions
    """

    mission: str = "imap"
    instrument: str = "mag"
    level: str | None = None
    descriptor: str | None = None
    content_date: datetime | None = None  # date data belongs to
    extension: str | None = None

    def supports_versioning(self) -> bool:
        return True

    def get_filename(self) -> str:
        if (
            not self.descriptor
            or not self.level
            or not self.content_date
            or not self.version
            or not self.extension
        ):
            logger.error(
                "No 'descriptor', 'content_date', 'version', or 'extension' defined. Cannot generate file name."
            )
            raise ValueError(
                "No 'descriptor', 'content_date', 'version', or 'extension' defined. Cannot generate file name."
            )

        return f"{self.mission}_{self.instrument}_{self.level}_{self.descriptor}_{self.content_date.strftime('%Y%m%d')}_v{self.version:03}.{self.extension}"

    def get_unversioned_pattern(self) -> re.Pattern:
        if (
            not self.content_date
            or not self.level
            or not self.descriptor
            or not self.extension
        ):
            logger.error(
                "No 'content_date', 'level', 'descriptor', or 'extension' defined. Cannot generate pattern."
            )
            raise ValueError(
                "No 'content_date', 'level', 'descriptor', or 'extension' defined. Cannot generate pattern."
            )

        return re.compile(
            rf"{self.mission}_{self.instrument}_{self.level}_{self.descriptor}_{self.content_date.strftime('%Y%m%d')}_v(?P<version>\d+)\.{self.extension}"
        )
