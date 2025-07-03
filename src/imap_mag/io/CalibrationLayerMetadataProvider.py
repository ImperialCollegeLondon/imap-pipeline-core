import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.IFileMetadataProvider import IFileMetadataProvider

logger = logging.getLogger(__name__)


@dataclass
class CalibrationLayerMetadataProvider(IFileMetadataProvider):
    """
    Metadata for calibration layers.
    Designed to handle the special internal case of calibration layers that do not obey exact SPDF metadata conventions.
    """

    mission: str = "imap"
    instrument: str = "mag"
    calibration_descriptor: str | None = None
    content_date: datetime | None = None  # date data belongs to
    extension: str | None = "json"

    def supports_versioning(self) -> bool:
        return True

    def get_folder_structure(self) -> str:
        if self.content_date is None:
            logger.error("No 'content_date' defined. Cannot generate folder structure.")
            raise ValueError(
                "No 'content_date' defined. Cannot generate folder structure."
            )

        return (
            Path("calibration") / "layers" / self.content_date.strftime("%Y/%m")
        ).as_posix()

    def get_filename(self):
        if (
            self.calibration_descriptor is None
            or self.content_date is None
            or self.version is None
            or self.extension is None
        ):
            logger.error(
                "No 'calibration_descriptor', 'content_date', or 'version' defined. Cannot generate file name."
            )
            raise ValueError(
                "No 'calibration_descriptor', 'content_date', or 'version' defined. Cannot generate file name."
            )
        date_str = self.content_date.strftime("%Y%m%d")
        return f"{self.mission}_{self.instrument}_{self.calibration_descriptor}-layer_{date_str}_v{self.version:03d}.{self.extension}"

    def get_unversioned_pattern(self) -> re.Pattern:
        """Get regex pattern for unversioned files."""

        if (
            not self.content_date
            or not self.calibration_descriptor
            or not self.extension
        ):
            logger.error(
                "No 'content_date', 'calibration_descriptor' or 'extension' defined. Cannot generate pattern."
            )
            raise ValueError("No 'content_date' defined. Cannot generate pattern.")

        return re.compile(
            rf"{self.mission}_{self.instrument}_{self.calibration_descriptor}-layer_{self.content_date.strftime('%Y%m%d')}_v(?P<version>\d+)\.{self.extension}"
        )

    @classmethod
    def from_filename(
        cls, filename: str | Path
    ) -> "CalibrationLayerMetadataProvider | None":
        """Create metadata provider from filename."""

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
