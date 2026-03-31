import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import ClassVar

from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler

logger = logging.getLogger(__name__)


@dataclass
class CalibrationLayerPathHandler(VersionedPathHandler):
    """
    Path handler for calibration layers.
    Designed to handle the special internal case of calibration layers that do not obey exact SPDF conventions.
    E.g filemnames like
        imap_mag_noop-norm-layer_20251017_v001.csv
        imap_mag_noop-norm-layer-data_20251017_v001.csv
    """

    mission: str = "imap"
    instrument: str = "mag"
    descriptor: str | None = None
    extra_descriptor: str = ""
    content_date: datetime | None = None  # date data belongs to
    extension: str = "json"

    DESCRIPTOR_WILDCARD: ClassVar[str] = "*"

    def get_folder_structure(self) -> str:
        super()._check_property_values("folder structure", ["content_date"])
        assert self.content_date

        return (
            Path("calibration") / "layers" / self.content_date.strftime("%Y/%m")
        ).as_posix()

    def get_content_date_for_indexing(self):
        return self.content_date

    def get_filename(self) -> str:
        super()._check_property_values("file name", ["descriptor", "content_date"])
        assert self.content_date

        return f"{self.mission}_{self.instrument}_{self.descriptor}-layer{self.extra_descriptor}_{self.content_date.strftime('%Y%m%d')}_v{self.version:03d}.{self.extension}"

    def get_unsequenced_pattern(self) -> re.Pattern:
        super()._check_property_values("pattern", ["descriptor", "content_date"])
        assert self.descriptor and self.content_date

        if self.descriptor == CalibrationLayerPathHandler.DESCRIPTOR_WILDCARD:
            full_descriptor = rf".+-layer{re.escape(self.extra_descriptor)}"
        else:
            full_descriptor = (
                f"{re.escape(self.descriptor)}-layer{re.escape(self.extra_descriptor)}"
            )

        return re.compile(
            rf"{self.mission}_{self.instrument}_{full_descriptor}_{self.content_date.strftime('%Y%m%d')}_v(?P<version>\d+)\.{self.extension}"
        )

    def get_equivalent_data_handler(self) -> "CalibrationLayerPathHandler":
        return CalibrationLayerPathHandler(
            descriptor=self.descriptor,
            extra_descriptor="-data",
            content_date=self.content_date,
            version=self.version,
            extension="csv",
        )

    @classmethod
    def from_filename(
        cls, filename: str | Path
    ) -> "CalibrationLayerPathHandler | None":
        match = re.match(
            r"imap_mag_(?P<descr>[^_]+)?-layer(?P<extra_descr>[^_]+)?_(?P<date>\d{8})_v(?P<version>\d+)\.(?P<ext>\w+)",
            Path(filename).name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with calibration regex."
        )

        if match is None:
            return None
        else:
            return cls(
                descriptor=match["descr"],
                extra_descriptor=match["extra_descr"] or "",
                content_date=datetime.strptime(match["date"], "%Y%m%d"),
                version=int(match["version"]),
                extension=match["ext"],
            )
