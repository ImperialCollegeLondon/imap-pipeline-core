import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from imap_mag.util import ScienceMode
from mag_toolkit.calibration import Sensor
from mag_toolkit.calibration.CalibrationDefinitions import CalibrationMethod

logger = logging.getLogger(__name__)


class Calibrator(ABC):
    data_store: Path | None = None

    @abstractmethod
    def get_handlers_of_files_needed_for_calibration(
        self, date: datetime, mode: ScienceMode, sensor: Sensor
    ) -> tuple[list, list]:
        """Get the path handlers of all files needed for calibration."""

    def needs_data_store(self):
        """
        Check if the calibrator needs a data store.
        :return: True, as most calibration requires a data store."""
        return True

    def setup_datastore(self, datastore: Path):
        """
        Setup the data store for the calibrator.
        :param datastore: The path to the data store.
        """
        self.data_store = datastore

    @abstractmethod
    def runCalibration(
        self, date: datetime, sciencefile: Path, calfile, datastore, config=None
    ) -> Path:
        """Calibration that generates a calibration layer."""


class IMAPLoCalibrator(Calibrator):
    def __init__(self):
        self.name = CalibrationMethod.IMAPLO_PIVOT

    def runCalibration(self, date, sciencefile, calfile, datastore, config=None):
        return Path()
