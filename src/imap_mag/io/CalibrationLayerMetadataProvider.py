import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.IFileMetadataProvider import IFileMetadataProvider

logger = logging.getLogger(__name__)


@dataclass
class CalibrationLayerMetadataProvider(IFileMetadataProvider):
    """
    Metadata for calibration layers.
    Designed to handle the special internal case of calibration layers that do not obey SPDF metadata conventions.
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
            Path(self.mission)
            / self.instrument
            / "calibration"
            / "layer"
            / self.content_date.strftime("%Y/%m")
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
