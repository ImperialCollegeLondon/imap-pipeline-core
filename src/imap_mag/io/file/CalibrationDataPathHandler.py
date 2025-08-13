import logging
from dataclasses import dataclass

from imap_mag.io.file.CalibrationLayerPathHandler import CalibrationLayerPathHandler

logger = logging.getLogger(__name__)


@dataclass
class CalibrationDataPathHandler(CalibrationLayerPathHandler):
    """
    Path handler for calibration layer science data.
    Designed to handle the special internal case of calibration layer science data.
    """

    @property
    def extra_descriptor(self) -> str:
        return "data"

    @property
    def extension(self) -> str:
        return "csv"
