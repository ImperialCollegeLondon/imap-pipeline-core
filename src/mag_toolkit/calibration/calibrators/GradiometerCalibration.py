import logging

import pytz

from imap_mag.config.CalibrationConfig import CalibrationConfig
from imap_mag.io import SciencePathHandler
from imap_mag.util import Level, ScienceMode
from mag_toolkit.calibration import CalibrationJobParameters
from mag_toolkit.calibration.CalibrationDefinitions import CalibrationMethod
from mag_toolkit.calibration.MatlabWrapper import call_matlab

from .CalibrationJob import CalibrationJob

logger = logging.getLogger(__name__)


class GradiometerCalibrationJob(CalibrationJob):
    mago_key = "mago_science_file"
    magi_key = "magi_science_file"

    def __init__(self, calibration_job_parameters: CalibrationJobParameters):
        super().__init__(calibration_job_parameters)
        self.name = CalibrationMethod.GRADIOMETER

        self.required_files[self.mago_key] = None
        self.required_files[self.magi_key] = None

    def _get_path_handlers(self, calibration_job_parameters: CalibrationJobParameters):
        mode = calibration_job_parameters.mode
        date = calibration_job_parameters.date

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

    def run_calibration(self, calfile, configuration: CalibrationConfig):
        """
        Run the gradiometry calibration.
        :param date: The date for which to run the calibration.
        :param sciencefile: The path to the science file.
        :param calfile: The path to the calibration file to be created.
        :param datastore: The path to the data store.
        :param config: Optional configuration for the calibration.
        :return: The path to the created calibration file."""

        dt_as_str = (
            self.calibration_job_parameters.date.astimezone(pytz.utc)
            .replace(tzinfo=None)
            .isoformat()
        )

        logger.info(f"Using datetime {dt_as_str}")

        if not self._check_environment_is_setup():
            raise FileNotFoundError(
                "Environment has not been correctly set up for calibration."
            )

        call_matlab(
            f'calibration.wrappers.run_gradiometry("{dt_as_str}", "{self.required_files[self.mago_key]}", "{self.required_files[self.magi_key]}", "{calfile}", "{self.data_store}", "{configuration.gradiometer.kappa!s}", "{configuration.gradiometer.sc_interference_threshold!s}")'
        )
        return calfile
