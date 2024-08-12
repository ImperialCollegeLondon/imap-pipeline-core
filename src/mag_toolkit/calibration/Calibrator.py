from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum

from . import MatlabWrapper
from .calibrationFormat import (
    CalibrationFormat,
    OffsetCollection,
    SingleCalibration,
    Unit,
)


class CalibratorType(str, Enum):
    SPINAXIS = "SpinAxisCalibrator"
    SPINPLANE = "SpinPlaneCalibrator"


class Calibrator(ABC):
    def generateOffsets(self, data) -> OffsetCollection:
        """Generates a set of offsets."""
        basicCalibration = self.runCalibration(data)

        offsetCollection = OffsetCollection(
            X=basicCalibration.x_offsets,
            Y=basicCalibration.y_offsets,
            Z=basicCalibration.z_offsets,
        )

        sensor_name = "MAGO"

        singleCalibration = SingleCalibration(
            timestamps=basicCalibration.timestamps,
            offsets=offsetCollection,
            units=Unit.NT,
            instrument=sensor_name,
            creation_timestamp=datetime.now(),
            method=str(self.name),
        )
        return singleCalibration

    def generateCalibration(self, data) -> CalibrationFormat:
        singleCalibration = self.generateOffsets(data)
        calibration = CalibrationFormat(
            valid_start=singleCalibration.timestamps[0],
            valid_end=singleCalibration.timestamps[-1],
            calibrations=[singleCalibration],
        )
        return calibration

    @abstractmethod
    def runCalibration(self, data):
        """Calibration that generates offsets and timestamps."""


class SpinAxisCalibrator(Calibrator):
    def __init__(self):
        self.name = CalibratorType.SPINAXIS

    def runCalibration(self, data):
        calibration = MatlabWrapper.simulateSpinAxisCalibration(data)

        return calibration


class SpinPlaneCalibrator(Calibrator):
    def __init__(self):
        self.name = CalibratorType.SPINPLANE

    def runCalibration(self, data):
        calibration = MatlabWrapper.simulateSpinPlaneCalibration()

        return calibration
