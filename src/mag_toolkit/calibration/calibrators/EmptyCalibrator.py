import logging
from pathlib import Path

import pytz

from imap_mag.io import StandardSPDFMetadataProvider
from imap_mag.util import Level, ScienceMode
from mag_toolkit.calibration.CalibrationDefinitions import CalibrationMethod
from mag_toolkit.calibration.MatlabWrapper import call_matlab

from .Calibrator import Calibrator

logger = logging.getLogger(__name__)


class EmptyCalibrator(Calibrator):
    science_file: str
    data_store: str

    def __init__(self):
        self.name = CalibrationMethod.NOOP

    def get_handlers_of_files_needed_for_calibration(self, date, mode, sensor):
        """
        Return path handler for every file this calibrator needs to calibrate"""
        level = Level.level_1b if mode == ScienceMode.Burst else Level.level_1c

        science_path_handler = StandardSPDFMetadataProvider(
            level=level.value,
            content_date=date,
            descriptor=f"{mode.short_name}-{sensor.value.lower()}",
            extension="cdf",
        )

        return [science_path_handler], []  # No other files needed for this calibrator

    def runCalibration(self, date, sciencefile: Path, calfile, datastore, config=None):
        # produce an epmpty calibration through matlab

        dt_as_str = date.astimezone(pytz.utc).replace(tzinfo=None).isoformat()

        logger.info(f"Using datetime {dt_as_str}")

        call_matlab(
            f'emptyCalibrator("{dt_as_str}", "{sciencefile}", "{calfile}", "{datastore}", "{config}")'
        )
        return calfile
