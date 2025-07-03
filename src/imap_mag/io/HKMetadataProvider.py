import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.StandardSPDFMetadataProvider import StandardSPDFMetadataProvider

logger = logging.getLogger(__name__)


@dataclass
class HKMetadataProvider(StandardSPDFMetadataProvider):
    """
    Metadata for HK files.
    """

    root_folder: str = "hk"
    ert: datetime | None = None  # date data was received by WebPODA

    def get_folder_structure(self) -> str:
        if not self.content_date or not self.level or not self.descriptor:
            logger.error(
                "No 'content_date', 'level', or 'descriptor' defined. Cannot generate folder structure."
            )
            raise ValueError(
                "No 'content_date', 'level', or 'descriptor' defined. Cannot generate folder structure."
            )

        return (
            Path(self.root_folder)
            / self.instrument
            / self.level
            / self.descriptor
            / self.content_date.strftime("%Y/%m")
        ).as_posix()

    @classmethod
    def from_filename(cls, filename: str | Path) -> "HKMetadataProvider | None":
        match = re.match(
            r"imap_mag_(?P<level>l\d)_(?P<descr>hsk-[^_]+)_(?P<date>\d{8})_v(?P<version>\d+)\.(?P<ext>\w+)",
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
                version=int(match["version"]),
                extension=match["ext"],
            )

    @staticmethod
    def convert_packet_to_descriptor(packet: str) -> str:
        """Convert HK packet name to metadata descriptor, used, e.g., in folder structures."""

        # Steps:
        # 1. Convert to lowercase (MAG_HSK_PW -> mag_hsk_pw)
        # 2. Replace underscores with hyphens (mag_hsk_pw -> mag-hsk-pw)
        # 3. Remove the prefix (mag-hsk-pw -> hsk-pw)
        return packet.lower().replace("_", "-").partition("-")[2]
