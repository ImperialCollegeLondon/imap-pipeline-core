import logging
from dataclasses import dataclass

from imap_mag.io.file.CalibrationLayerPathHandler import CalibrationLayerPathHandler

logger = logging.getLogger(__name__)


@dataclass
class CalibrationMetadataPathHandler(CalibrationLayerPathHandler):
    """
    Path handler for calibration layer metadata.
    Designed to handle the special internal case of calibration layer metadata.
    """

    @property
    def extra_descriptor(self) -> str:
        return "meta"

    @property
    def extension(self) -> str:
        return "json"
