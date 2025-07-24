import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.file.HKPathHandler import HKPathHandler
from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler
from imap_mag.util import HKLevel

logger = logging.getLogger(__name__)


@dataclass
class HKDecodedPathHandler(VersionedPathHandler, HKPathHandler):
    """
    Path handler for HK files.
    """

    @property
    def level(self) -> str:
        return HKLevel.l1.value

    def get_filename(self) -> str:
        if not self.descriptor or not self.content_date or not self.extension:
            logger.error(
                "No 'descriptor', 'content_date', or 'extension' defined. Cannot generate file name."
            )
            raise ValueError(
                "No 'descriptor', 'content_date', or 'extension' defined. Cannot generate file name."
            )

        return f"{self.mission}_{self.instrument}_{self.level}_{self.descriptor}_{self.content_date.strftime('%Y%m%d')}_v{self.version:03}.{self.extension}"  # type: ignore

    def get_unsequenced_pattern(self) -> re.Pattern:
        if not self.content_date or not self.descriptor or not self.extension:
            logger.error(
                "No 'content_date', 'descriptor', or 'extension' defined. Cannot generate pattern."
            )
            raise ValueError(
                "No 'content_date', 'descriptor', or 'extension' defined. Cannot generate pattern."
            )

        return re.compile(
            rf"{self.mission}_{self.instrument}_{self.level}_{self.descriptor}_{self.content_date.strftime('%Y%m%d')}_v(?P<version>\d+)\.{self.extension}"
        )

    @classmethod
    def from_filename(cls, filename: str | Path) -> "HKDecodedPathHandler | None":
        allowed_hk_descriptors: set[str] = super()._get_allowed_descriptors()
        logger.debug(f"Allowed HK descriptors: {', '.join(allowed_hk_descriptors)}")

        match = re.match(
            rf"imap_mag_{HKLevel.l1.value}_(?P<descr>(?:{'|'.join(allowed_hk_descriptors)})-[^_]+)_(?P<date>\d{{8}})_v(?P<version>\d+)\.(?P<ext>\w+)",
            Path(filename).name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with HK regex."
        )

        if match is None:
            return None
        else:
            return cls(
                descriptor=match["descr"],
                content_date=datetime.strptime(match["date"], "%Y%m%d"),
                version=int(match["version"]),
                extension=match["ext"],
            )
