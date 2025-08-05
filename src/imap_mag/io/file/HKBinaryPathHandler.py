import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, overload

from imap_mag.io.file.HKPathHandler import HKPathHandler
from imap_mag.io.file.PartitionedPathHandler import PartitionedPathHandler
from imap_mag.io.file.SequenceablePathHandler import UnsequencedStyle
from imap_mag.util import HKLevel

logger = logging.getLogger(__name__)


@dataclass
class HKBinaryPathHandler(PartitionedPathHandler, HKPathHandler):
    """
    Path handler for HK files.
    """

    ert: datetime | None = None  # date data was received by WebPODA

    @property
    def level(self) -> str:
        return HKLevel.l0.value

    def get_filename(self) -> str:
        super()._check_property_values(
            "file name", ["descriptor", "content_date", "extension"]
        )
        assert self.content_date

        return f"{self.mission}_{self.instrument}_{self.level}_{self.descriptor}_{self.content_date.strftime('%Y%m%d')}_{self.part:03}.{self.extension}"

    @overload
    def get_unsequenced_pattern(
        self, style: Literal[UnsequencedStyle.Regex]
    ) -> re.Pattern:
        pass

    @overload
    def get_unsequenced_pattern(self, style: Literal[UnsequencedStyle.SQL]) -> str:
        pass

    def get_unsequenced_pattern(
        self, style: UnsequencedStyle = UnsequencedStyle.Regex
    ) -> re.Pattern | str:
        super()._check_property_values(
            "pattern", ["descriptor", "content_date", "extension"]
        )
        assert self.content_date

        prefix = f"{self.mission}_{self.instrument}_{self.level}_{self.descriptor}_{self.content_date.strftime('%Y%m%d')}_"
        suffix = f".{self.extension}"

        match style:
            case UnsequencedStyle.Regex:
                return re.compile(
                    rf"{re.escape(prefix)}(?P<part>\d+){re.escape(suffix)}"
                )
            case UnsequencedStyle.SQL:
                return f"{prefix}%{suffix}"

    @classmethod
    def from_filename(cls, filename: str | Path) -> "HKBinaryPathHandler | None":
        allowed_hk_descriptors: set[str] = super()._get_allowed_descriptors()
        logger.debug(f"Allowed HK descriptors: {', '.join(allowed_hk_descriptors)}")

        match = re.match(
            rf"imap_mag_{HKLevel.l0.value}_(?P<descr>(?:{'|'.join(allowed_hk_descriptors)})-[^_]+)_(?P<date>\d{{8}})_(?P<part>\d+)\.(?P<ext>\w+)",
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
                part=int(match["part"]),
                extension=match["ext"],
            )
