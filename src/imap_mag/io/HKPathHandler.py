import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.StandardSPDFPathHandler import StandardSPDFPathHandler
from imap_mag.util import HKPacket

logger = logging.getLogger(__name__)


@dataclass
class HKPathHandler(StandardSPDFPathHandler):
    """
    Path handler for HK files.
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
    def from_filename(cls, filename: str | Path) -> "HKPathHandler | None":
        allowed_hk_descriptors: set[str] = {
            cls.convert_packet_to_descriptor(hk.packet).partition("-")[0]
            for hk in HKPacket
        }
        logger.debug(f"Allowed HK descriptors: {', '.join(allowed_hk_descriptors)}")

        match = re.match(
            rf"imap_mag_(?P<level>l\d)_(?P<descr>(?:{'|'.join(allowed_hk_descriptors)})-[^_]+)_(?P<date>\d{{8}})_v(?P<version>\d+)\.(?P<ext>\w+)",
            Path(filename).name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with HK regex."
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
