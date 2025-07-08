import logging
from pathlib import Path

import pytz

from imap_mag.io import StandardSPDFMetadataProvider
from imap_mag.util import Level, ScienceMode
from mag_toolkit.calibration.CalibrationDefinitions import CalibrationMethod
from mag_toolkit.calibration.MatlabWrapper import call_matlab

from .Calibrator import Calibrator

logger = logging.getLogger(__name__)


class GradiometerCalibrator(Calibrator):
    magi_science_file: Path
    mago_science_file: Path

    def __init__(self):
        self.name = CalibrationMethod.GRADIOMETRY

    def add_file(file: Path):
        """
        Add a file to the calibrator.
        :param file: The path to the file to be added.
        """

        pass

    def get_handlers_of_files_needed_for_calibration(self, date, mode, sensor):
        """
        Get the handlers of files needed for gradiometry calibration.
        :param date: The date for which to get the handlers.
        :param mode: The science mode.
        :param sensor: The sensor type.
        :return: A tuple containing lists of science and other path handlers."""

        level = Level.level_1b if mode == ScienceMode.Burst else Level.level_1c

        mago_science_path = StandardSPDFMetadataProvider(
            level=level.value,
            content_date=date,
            descriptor=f"{mode.short_name}-mago",
            extension="cdf",
        )

        magi_science_path = StandardSPDFMetadataProvider(
            level=level.value,
            content_date=date,
            descriptor=f"{mode.short_name}-mago",
            extension="cdf",
        )

        return [
            mago_science_path,
            magi_science_path,
        ], []  # No other files needed for this calibrator

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
