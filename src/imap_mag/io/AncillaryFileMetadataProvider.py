import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.StandardSPDFMetadataProvider import StandardSPDFMetadataProvider

logger = logging.getLogger(__name__)


@dataclass
class AncillaryFileMetadataProvider(StandardSPDFMetadataProvider):
    """
    Metadata for ancillary files
    Extends the SPDF standard metadata provider to include an end date.
    """

    end_date: datetime | None = None  # end date of validity for ancillary files

    def get_filename(self) -> str:
        if (
            self.descriptor is None
            or self.content_date is None
            or self.version is None
            or self.extension is None
        ):
            logger.error(
                "No 'descriptor', 'content_date', 'version', or 'extension' defined. Cannot generate file name."
            )
            raise ValueError(
                "No 'descriptor', 'content_date', 'version', or 'extension' defined. Cannot generate file name."
            )

        descriptor = self.descriptor
        if self.level is not None:
            descriptor = f"{self.level}_{descriptor}"

        if self.end_date is None:
            valid_date_range = self.content_date.strftime("%Y%m%d")
        else:
            valid_date_range = f"{self.content_date.strftime('%Y%m%d')}_{self.end_date.strftime('%Y%m%d')}"

        return f"{self.mission}_{self.instrument}_{descriptor}_{valid_date_range}_v{self.version:03}.{self.extension}"

    def get_unversioned_pattern(self) -> re.Pattern:
        """Get regex pattern for unversioned files."""

        if not self.content_date or not self.descriptor or not self.extension:
            logger.error(
                "No 'content_date' or 'descriptor' or 'extension' defined. Cannot generate pattern."
            )
            raise ValueError(
                "No 'content_date' or 'descriptor' or 'extension' defined. Cannot generate pattern."
            )

        descriptor = self.descriptor

        if self.level is not None:
            descriptor = f"{self.level}_{descriptor}"

        if self.end_date is None:
            valid_date_range = self.content_date.strftime("%Y%m%d")
        else:
            valid_date_range = f"{self.content_date.strftime('%Y%m%d')}_{self.end_date.strftime('%Y%m%d')}"

        return re.compile(
            rf"{self.mission}_{self.instrument}_{descriptor}_{valid_date_range}_v(?P<version>\d+)\.{self.extension}"
        )

    @classmethod
    def from_filename(
        cls, filename: str | Path
    ) -> "AncillaryFileMetadataProvider | None":
        """Create metadata provider from filename."""

        match = re.match(
            r"imap_mag_((?P<level>l\d[a-zA-Z]?(-pre)?)_)?(?P<descr>[^_]+)_(?P<date>\d{8})_((?P<enddate>\d{8})_)?v(?P<version>\d+)\.(?P<ext>\w+)",
            Path(filename).name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with SPDF standard regex."
        )

        if match is None:
            return None
        else:
            return cls(
                level=match["level"],
                descriptor=match["descr"],
                content_date=datetime.strptime(match["date"], "%Y%m%d"),
                end_date=datetime.strptime(match["enddate"], "%Y%m%d")
                if match["enddate"]
                else None,
                version=int(match["version"]),
                extension=match["ext"],
            )
