import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from mag_toolkit.calibration.CalibrationDefinitions import CalibrationMethod

logger = logging.getLogger(__name__)


class Calibrator(ABC):
    @abstractmethod
    def runCalibration(
        self, date: datetime, sciencefile: Path, calfile, datastore, config=None
    ) -> Path:
        """Calibration that generates a calibration layer."""


class SpinAxisCalibrator(Calibrator):
    def __init__(self):
        self.name = CalibrationMethod.LEINWEBER

    def runCalibration(
        self, date, sciencefile, calfile, datastore, config=None
    ) -> Path:
        return Path()


class SpinPlaneCalibrator(Calibrator):
    def __init__(self):
        self.name = CalibrationMethod.KEPKO

    def runCalibration(self, date, sciencefile, calfile, datastore, config=None):
        return Path()


class IMAPLoCalibrator(Calibrator):
    def __init__(self):
        self.name = CalibrationMethod.IMAPLO_PIVOT

    def runCalibration(self, date, sciencefile, calfile, datastore, config=None):
        return Path()
