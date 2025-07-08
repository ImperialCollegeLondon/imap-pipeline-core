import logging

import pytz

from imap_mag.io import SciencePathHandler
from imap_mag.util import Level, ScienceMode
from mag_toolkit.calibration.CalibrationDefinitions import CalibrationMethod
from mag_toolkit.calibration.MatlabWrapper import call_matlab

from .Calibrator import Calibrator

logger = logging.getLogger(__name__)


class EmptyCalibrator(Calibrator):
    science_file_key = "science_file"

    def __init__(self, date, mode, sensor):
        self.name = CalibrationMethod.NOOP
        self.date = date
        self.mode = mode
        self.sensor = sensor
        self.required_files[self.science_file_key] = None

    def _get_path_handlers(self, date, mode, sensor):
        path_handlers = {}
        level = (
            Level.ScienceLevel.l1b
            if mode == ScienceMode.Burst
            else Level.ScienceLevel.l1c
        )

        science_file = SciencePathHandler(
            level=level.value,
            content_date=date,
            descriptor=f"{mode.short_name}-mago",
            extension="cdf",
        )

        path_handlers[self.science_file_key] = science_file

        return path_handlers

    def run_calibration(self, calfile, config=None):
        # produce an epmpty calibration through matlab

        dt_as_str = self.date.astimezone(pytz.utc).replace(tzinfo=None).isoformat()

        logger.info(f"Using datetime {dt_as_str}")

        call_matlab(
            f'calibration.wrappers.run_empty_calibrator("{dt_as_str}", "{self.required_files[self.science_file_key]}", "{calfile}", "{self.data_store}", "{config}")'
        )

        return calfile
