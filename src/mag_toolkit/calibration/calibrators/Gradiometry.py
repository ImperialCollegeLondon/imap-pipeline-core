import logging
from pathlib import Path

import pytz

from mag_toolkit.calibration.CalibrationDefinitions import CalibrationMethod
from mag_toolkit.calibration.MatlabWrapper import call_matlab

from .Calibrator import Calibrator

logger = logging.getLogger(__name__)


class GradiometerCalibrator(Calibrator):
    def __init__(self):
        self.name = CalibrationMethod.GRADIOMETRY

    def runCalibration(self, date, sciencefile: Path, calfile, datastore, config=None):
        """
        Run the gradiometry calibration.
        :param date: The date for which to run the calibration.
        :param sciencefile: The path to the science file.
        :param calfile: The path to the calibration file to be created.
        :param datastore: The path to the data store.
        :param config: Optional configuration for the calibration.
        :return: The path to the created calibration file."""

        dt_as_str = date.astimezone(pytz.utc).replace(tzinfo=None).isoformat()

        logger.info(f"Using datetime {dt_as_str}")

        call_matlab(
            f'run_gradiometry("{dt_as_str}", "{sciencefile}", "{calfile}", "{datastore}", "{config}")'
        )
        return calfile
