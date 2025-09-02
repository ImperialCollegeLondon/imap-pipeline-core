import abc
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.file.IFilePathHandler import IFilePathHandler
from imap_mag.util import HKPacket

logger = logging.getLogger(__name__)


@dataclass
class HKPathHandler(IFilePathHandler):
    """
    Metadata for HK path handlers.
    """

    root_folder: str = "hk"
    mission: str = "imap"
    instrument: str = "mag"
    descriptor: str | None = None
    content_date: datetime | None = None  # date data belongs to
    extension: str | None = None

    @property
    @abc.abstractmethod
    def level(self) -> str:
        """HK level for the handler. It cannot be set."""
        pass

    def get_date_for_indexing(self):
        return self.content_date

    def get_folder_structure(self) -> str:
        super()._check_property_values(
            "folder structure", ["descriptor", "content_date"]
        )
        assert self.descriptor and self.content_date

        return (
            Path(self.root_folder)
            / self.instrument
            / self.level
            / self.descriptor
            / self.content_date.strftime("%Y/%m")
        ).as_posix()

    @staticmethod
    def _get_allowed_descriptors() -> set[str]:
        """Get allowed HK descriptors based on the HKPacket enumeration."""

        return {
            HKPathHandler.convert_packet_to_descriptor(hk.packet).partition("-")[0]
            for hk in HKPacket
        }

    @staticmethod
    def convert_packet_to_descriptor(packet: str) -> str:
        """
        Convert HK packet name to metadata descriptor, used, e.g., in folder structures.

        Steps:
            1. Convert to lowercase (MAG_HSK_PW -> mag_hsk_pw)
            2. Replace underscores with hyphens (mag_hsk_pw -> mag-hsk-pw)
            3. Remove the prefix (mag-hsk-pw -> hsk-pw)

        Example:
            Input: 'MAG_HSK_PW'
            Output: 'hsk-pw'
        """

        return packet.lower().replace("_", "-").partition("-")[2]
