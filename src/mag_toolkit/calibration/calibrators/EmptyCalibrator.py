import logging
from pathlib import Path

import pytz

from mag_toolkit.calibration.CalibrationDefinitions import CalibrationMethod
from mag_toolkit.calibration.MatlabWrapper import call_matlab

from .Calibrator import Calibrator

logger = logging.getLogger(__name__)


class EmptyCalibrator(Calibrator):
    def __init__(self):
        self.name = CalibrationMethod.NOOP

    def runCalibration(self, date, sciencefile: Path, calfile, datastore, config=None):
        # produce an epmpty calibration through matlab

        dt_as_str = date.astimezone(pytz.utc).replace(tzinfo=None).isoformat()

        logger.info(f"Using datetime {dt_as_str}")

        call_matlab(
            f'emptyCalibrator("{dt_as_str}", "{sciencefile}", "{calfile}", "{datastore}", "{config}")'
        )
        return calfile
