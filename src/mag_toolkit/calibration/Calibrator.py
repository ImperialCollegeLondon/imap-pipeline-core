from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path


class CalibrationMethod(str, Enum):
    KEPKO = "Kepko"
    LEINWEBER = "Leinweber"
    IMAPLO_PIVOT = "IMAP-Lo Pivot Platform Interference"
    SUM = "Sum of other calibrations"


class Calibrator(ABC):
    @abstractmethod
    def runCalibration(self, data) -> Path:
        """Calibration that generates a calibration layer."""


class SpinAxisCalibrator(Calibrator):
    def __init__(self):
        self.name = CalibrationMethod.LEINWEBER

    def runCalibration(self, data) -> Path:
        return Path()


class SpinPlaneCalibrator(Calibrator):
    def __init__(self):
        self.name = CalibrationMethod.KEPKO

    def runCalibration(self, data):
        return Path()
