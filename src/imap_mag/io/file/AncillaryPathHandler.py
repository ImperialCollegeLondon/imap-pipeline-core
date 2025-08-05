import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, overload

from imap_mag.io.file.SequenceablePathHandler import UnsequencedStyle
from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler

logger = logging.getLogger(__name__)


@dataclass
class AncillaryPathHandler(VersionedPathHandler):
    """
    Path handler for ancillary files
    """

    mission: str = "imap"
    instrument: str = "mag"
    descriptor: str | None = None
    start_date: datetime | None = None  # start date of validity
    end_date: datetime | None = None  # end date of validity
    extension: str | None = None

    def get_sub_folder(self) -> Path:
        """Get the subfolder for ancillary files."""

        super()._check_property_values("subfolder", ["descriptor", "start_date"])
        assert self.descriptor and self.start_date

        match self.descriptor:
            case "ialirt-calibration":
                return Path("ialirt")
            case "l1d-calibration":
                return Path("l1d")
            case "l2-calibration":
                return Path("l2-rotation")
            case "l1b-calibration":
                return Path("l1b")
            case _:
                if self.descriptor.endswith("-offsets"):
                    return Path("l2-offsets") / self.start_date.strftime("%Y/%m")
                else:
                    logger.error(
                        f"Unknown descriptor '{self.descriptor}' for ancillary files."
                    )
                    raise ValueError(
                        f"Unknown descriptor '{self.descriptor}' for ancillary files."
                    )

    def get_folder_structure(self) -> str:
        super()._check_property_values("folder structure", ["start_date"])
        assert self.start_date

        return (Path("science-ancillary") / self.get_sub_folder()).as_posix()

    def get_filename(self) -> str:
        super()._check_property_values(
            "file name", ["descriptor", "start_date", "extension"]
        )
        assert self.start_date

        if self.end_date is None:
            valid_date_range = self.start_date.strftime("%Y%m%d")
        else:
            valid_date_range = f"{self.start_date.strftime('%Y%m%d')}_{self.end_date.strftime('%Y%m%d')}"

        return f"{self.mission}_{self.instrument}_{self.descriptor}_{valid_date_range}_v{self.version:03}.{self.extension}"

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
            "pattern", ["descriptor", "start_date", "extension"]
        )
        assert self.start_date

        if self.end_date is None:
            valid_date_range = self.start_date.strftime("%Y%m%d")
        else:
            valid_date_range = f"{self.start_date.strftime('%Y%m%d')}_{self.end_date.strftime('%Y%m%d')}"

        prefix = (
            f"{self.mission}_{self.instrument}_{self.descriptor}_{valid_date_range}_"
        )
        suffix = f".{self.extension}"

        match style:
            case UnsequencedStyle.Regex:
                return re.compile(
                    rf"{re.escape(prefix)}v(?P<version>\d+){re.escape(suffix)}"
                )
            case UnsequencedStyle.SQL:
                return f"{prefix}v%{suffix}"

    @classmethod
    def from_filename(cls, filename: str | Path) -> "AncillaryPathHandler | None":
        match = re.match(
            r"imap_mag_(?P<descr>[^_]+(-calibration|-offsets))_(?P<start>\d{8})_((?P<end>\d{8})_)?v(?P<version>\d+)\.(?P<ext>\w+)",
            Path(filename).name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with ancillary file regex."
        )

        if match is None:
            return None
        else:
            return cls(
                descriptor=match["descr"],
                start_date=datetime.strptime(match["start"], "%Y%m%d"),
                end_date=datetime.strptime(match["end"], "%Y%m%d")
                if match["end"]
                else None,
                version=int(match["version"]),
                extension=match["ext"],
            )
