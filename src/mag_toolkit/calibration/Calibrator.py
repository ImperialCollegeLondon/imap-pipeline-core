from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path

from mag_toolkit.calibration.MatlabWrapper import call_matlab


class CalibrationMethod(str, Enum):
    KEPKO = "Kepko"
    LEINWEBER = "Leinweber"
    IMAPLO_PIVOT = "IMAP-Lo Pivot Platform Interference"
    NOOP = "noop"
    SUM = "Sum of other calibrations"


class Calibrator(ABC):
    @abstractmethod
    def runCalibration(
        self, date, sciencefile: Path, calfile, datastore, config=None
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


class EmptyCalibrator(Calibrator):
    def __init__(self):
        self.name = CalibrationMethod.NOOP

    def runCalibration(self, date, sciencefile: Path, calfile, datastore, config=None):
        # produce an epmpty calibration through matlab

        call_matlab(
            f'emptyCalibrator("{date}", "{sciencefile}", "{calfile}", "{datastore}", "{config}")'
        )
        return calfile
