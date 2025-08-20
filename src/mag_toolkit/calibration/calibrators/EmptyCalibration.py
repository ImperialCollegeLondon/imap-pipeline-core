import logging
from pathlib import Path

import pytz

from imap_mag.io.file import CalibrationLayerPathHandler, SciencePathHandler
from imap_mag.util import Level, ScienceMode
from mag_toolkit.calibration import CalibrationJobParameters
from mag_toolkit.calibration.CalibrationDefinitions import CalibrationMethod
from mag_toolkit.calibration.MatlabWrapper import call_matlab

from .CalibrationJob import CalibrationJob

logger = logging.getLogger(__name__)


class EmptyCalibrationJob(CalibrationJob):
    science_file_key = "science_file"

    def __init__(
        self, calibration_job_parameters: CalibrationJobParameters, work_folder: Path
    ):
        super().__init__(calibration_job_parameters, work_folder)
        self.name = CalibrationMethod.NOOP
        self.required_files[self.science_file_key] = None

    def _get_path_handlers(self, calibration_job_parameters: CalibrationJobParameters):
        mode = calibration_job_parameters.mode
        sensor = calibration_job_parameters.sensor
        date = calibration_job_parameters.date
        path_handlers = {}
        level = (
            Level.ScienceLevel.l1b
            if mode == ScienceMode.Burst
            else Level.ScienceLevel.l1c
        )

        science_file = SciencePathHandler(
            level=level.value,
            content_date=date,
            descriptor=f"{mode.short_name}-{sensor.value.lower()}",
            extension="cdf",
        )

        path_handlers[self.science_file_key] = science_file

        return path_handlers

    def run_calibration(
        self, cal_handler: CalibrationLayerPathHandler, config=None
    ) -> tuple[Path, Path]:
        # produce an empty calibration through matlab

        dt_as_str = (
            self.calibration_job_parameters.date.astimezone(pytz.utc)
            .replace(tzinfo=None)
            .isoformat()
        )
        calfile = self.work_folder / cal_handler.get_filename()
        datafile = (
            self.work_folder / cal_handler.get_equivalent_data_handler().get_filename()
        )

        logger.info(f"Using datetime {dt_as_str}")

        call_matlab(
            f'calibration.wrappers.run_empty_calibrator("{dt_as_str}", "{self.required_files[self.science_file_key]}", "{calfile}", "{datafile}", "{self.data_store}", "")'
        )

        return calfile, datafile
