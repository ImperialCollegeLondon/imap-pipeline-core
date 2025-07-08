import logging

import pytz

from imap_mag.io import SciencePathHandler
from imap_mag.util import Level, ScienceMode
from mag_toolkit.calibration.CalibrationDefinitions import CalibrationMethod
from mag_toolkit.calibration.MatlabWrapper import call_matlab

from .Calibrator import Calibrator

logger = logging.getLogger(__name__)


class GradiometerCalibrator(Calibrator):
    mago_key = "mago_science_file"
    magi_key = "magi_science_file"

    kappa = 0.0
    sc_interference_threshold = 10.0

    def __init__(self, date, mode, sensor):
        self.date = date
        self.mode = mode
        self.sensor = sensor
        self.name = CalibrationMethod.GRADIOMETER

        self.required_files[self.mago_key] = None
        self.required_files[self.magi_key] = None

    def _get_path_handlers(self, date, mode, sensor):
        path_handlers = {}
        level = (
            Level.ScienceLevel.l1b
            if mode == ScienceMode.Burst
            else Level.ScienceLevel.l1c
        )

        mago_science_path = SciencePathHandler(
            level=level.value,
            content_date=date,
            descriptor=f"{mode.short_name}-mago",
            extension="cdf",
        )

        magi_science_path = SciencePathHandler(
            level=level.value,
            content_date=date,
            descriptor=f"{mode.short_name}-magi",
            extension="cdf",
        )

        path_handlers[self.mago_key] = mago_science_path
        path_handlers[self.magi_key] = magi_science_path

        return path_handlers

    def run_calibration(self, calfile, config=None):
        """
        Run the gradiometry calibration.
        :param date: The date for which to run the calibration.
        :param sciencefile: The path to the science file.
        :param calfile: The path to the calibration file to be created.
        :param datastore: The path to the data store.
        :param config: Optional configuration for the calibration.
        :return: The path to the created calibration file."""

        dt_as_str = self.date.astimezone(pytz.utc).replace(tzinfo=None).isoformat()

        logger.info(f"Using datetime {dt_as_str}")

        if not self._check_for_required_files():
            raise FileNotFoundError(
                "Required files for gradiometry calibration are missing."
            )

        call_matlab(
            f'calibration.wrappers.run_gradiometry("{dt_as_str}", "{self.required_files[self.mago_key]}", "{self.required_files[self.magi_key]}", "{calfile}", "{self.data_store}", "{self.kappa!s}", "{self.sc_interference_threshold!s}")'
        )
        return calfile
