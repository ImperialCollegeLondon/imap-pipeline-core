import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.IFileMetadataProvider import IFileMetadataProvider

logger = logging.getLogger(__name__)


@dataclass
class StandardSPDFMetadataProvider(IFileMetadataProvider):
    """
    Metadata for standard SPDF files.
    See: https://imap-processing.readthedocs.io/en/latest/data-access/naming-conventions.html#data-product-science-file-naming-conventions
    """

    prefix: str | None = "imap_mag"
    level: str | None = None
    descriptor: str | None = None
    content_date: datetime | None = None  # date data belongs to
    extension: str | None = None

    def supports_versioning(self) -> bool:
        return True

    def get_folder_structure(self) -> str:
        if self.content_date is None:
            logger.error("No 'content_date' defined. Cannot generate folder structure.")
            raise ValueError(
                "No 'content_date' defined. Cannot generate folder structure."
            )

        return self.content_date.strftime("%Y/%m/%d")

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

        if self.prefix is not None:
            descriptor = f"{self.prefix}_{descriptor}"

        return f"{descriptor}_{self.content_date.strftime('%Y%m%d')}_v{self.version:03}.{self.extension}"

    @classmethod
    def from_filename(
        cls, filename: str | Path
    ) -> "StandardSPDFMetadataProvider | None":
        """Create metadata provider from filename."""

        match = re.match(
            r"(?P<prefix>imap_mag)?_?(?P<level>l\d[a-zA-Z]?)?_?(?P<descr>[^_]+)_(?P<date>\d{8})_v(?P<version>\d+)\.(?P<ext>\w+)",
            Path(filename).name,
        )
        logger.debug(f"Filename {filename} matches {match} with SPDF standard regex.")

        if match is None:
            return None
        else:
            return cls(
                prefix=match["prefix"],
                level=match["level"],
                descriptor=match["descr"],
                content_date=datetime.strptime(match["date"], "%Y%m%d"),
                version=int(match["version"]),
                extension=match["ext"],
            )
