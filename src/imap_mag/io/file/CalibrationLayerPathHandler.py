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
class CalibrationLayerPathHandler(VersionedPathHandler):
    """
    Path handler for calibration layers.
    Designed to handle the special internal case of calibration layers that do not obey exact SPDF conventions.
    """

    mission: str = "imap"
    instrument: str = "mag"
    calibration_descriptor: str | None = None
    content_date: datetime | None = None  # date data belongs to
    extension: str | None = "json"

    def get_folder_structure(self) -> str:
        super()._check_property_values("folder structure", ["content_date"])
        assert self.content_date

        return (
            Path("calibration") / "layers" / self.content_date.strftime("%Y/%m")
        ).as_posix()

    def get_filename(self):
        super()._check_property_values(
            "file name", ["calibration_descriptor", "content_date", "extension"]
        )
        assert self.content_date

        return f"{self.mission}_{self.instrument}_{self.calibration_descriptor}-layer_{self.content_date.strftime('%Y%m%d')}_v{self.version:03d}.{self.extension}"

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
            "pattern", ["calibration_descriptor", "content_date", "extension"]
        )
        assert self.content_date

        prefix = f"{self.mission}_{self.instrument}_{self.calibration_descriptor}-layer_{self.content_date.strftime('%Y%m%d')}_"
        suffix = f".{self.extension}"

        match style:
            case UnsequencedStyle.Regex:
                return re.compile(
                    rf"{re.escape(prefix)}v(?P<version>\d+){re.escape(suffix)}"
                )
            case UnsequencedStyle.SQL:
                return f"{prefix}v%{suffix}"

    @classmethod
    def from_filename(
        cls, filename: str | Path
    ) -> "CalibrationLayerPathHandler | None":
        match = re.match(
            r"imap_mag_(?P<descr>[^_]+)?-layer_(?P<date>\d{8})_v(?P<version>\d+)\.(?P<ext>\w+)",
            Path(filename).name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with calibration regex."
        )

        if match is None:
            return None
        else:
            return cls(
                mission="imap",
                instrument="mag",
                calibration_descriptor=match["descr"],
                content_date=datetime.strptime(match["date"], "%Y%m%d"),
                version=int(match["version"]),
                extension=match["ext"],
            )
