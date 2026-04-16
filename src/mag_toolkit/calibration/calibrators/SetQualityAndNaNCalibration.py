import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from imap_mag.config.CalibrationConfig import CalibrationConfig
from imap_mag.io.file import CalibrationLayerPathHandler, SciencePathHandler
from imap_mag.io.FileFinder import FileFinder
from imap_mag.util import Level, ScienceMode
from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
    CalibrationMetadata,
    CalibrationMethod,
    Mission,
    ValueType,
)
from mag_toolkit.calibration.CalibrationJobParameters import CalibrationJobParameters
from mag_toolkit.calibration.CalibrationLayer import CalibrationLayer
from mag_toolkit.calibration.Layer import Validity
from mag_toolkit.calibration.ScienceLayer import ScienceLayer

from .CalibrationJob import CalibrationJob

logger = logging.getLogger(__name__)


class SetQualityAndNaNCalibrationJob(CalibrationJob):
    science_file_key = "science_file"

    def __init__(
        self,
        calibration_job_parameters: CalibrationJobParameters,
        work_folder: Path,
        file_finder: FileFinder | None = None,
    ):
        super().__init__(calibration_job_parameters, work_folder)
        self.name = CalibrationMethod.SET_QUALITY_AND_NAN
        self.required_files[self.science_file_key] = None
        self._finder = file_finder or FileFinder(work_folder, work_folder, None)

    def _get_path_handlers(self, calibration_job_parameters: CalibrationJobParameters):
        mode = calibration_job_parameters.mode
        sensor = calibration_job_parameters.sensor
        date = calibration_job_parameters.date
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

        return {self.science_file_key: science_file}

    def needs_data_store(self):
        return True

    def run_calibration(
        self, cal_handler: CalibrationLayerPathHandler, config: CalibrationConfig
    ) -> tuple[Path, Path]:
        if config.set_quality_and_nan is None:
            raise ValueError(
                "SetQualityAndNaN calibration requires a set_quality_and_nan configuration "
                "with a csv_file path."
            )

        csv_path = self._finder.find_by_name_or_path(
            config.set_quality_and_nan.csv_file, throw_if_not_found=True
        )

        input_df = pd.read_csv(
            csv_path,
            dtype={
                "nan_x": bool,
                "nan_y": bool,
                "nan_z": bool,
                "quality_flag": pd.Int64Dtype(),
                "quality_bitmask": pd.Int64Dtype(),
            },
            parse_dates=["start_date", "end_date"],
        )

        date = self.calibration_job_parameters.date

        # Get actual science data time range from the science file
        science_file = self.required_files.get(self.science_file_key)
        science_start, science_end = self._get_science_time_range(science_file)

        day_start = pd.Timestamp(datetime(date.year, date.month, date.day))
        day_end = day_start + pd.Timedelta(days=1)
        science_start_ts = pd.Timestamp(science_start)
        science_end_ts = pd.Timestamp(science_end)

        # Filter to rows overlapping both the logical day and the science data range
        overlaps = (
            (input_df["start_date"] <= science_end_ts)
            & (input_df["end_date"] >= science_start_ts)
            & (input_df["start_date"] < day_end)
            & (input_df["end_date"] > day_start)
        )
        filtered = input_df[overlaps].copy()

        schema = {
            CONSTANTS.CSV_VARS.EPOCH: "datetime64[ns]",
            CONSTANTS.CSV_VARS.OFFSET_X: "float64",
            CONSTANTS.CSV_VARS.OFFSET_Y: "float64",
            CONSTANTS.CSV_VARS.OFFSET_Z: "float64",
            CONSTANTS.CSV_VARS.TIMEDELTA: "float64",
            CONSTANTS.CSV_VARS.QUALITY_FLAG: pd.Int64Dtype(),
            CONSTANTS.CSV_VARS.QUALITY_BITMASK: pd.Int64Dtype(),
        }

        filtered["window_start"] = filtered["start_date"].clip(lower=day_start)
        filtered["window_end"] = filtered["end_date"].clip(upper=science_end_ts)
        end_filtered = filtered[filtered["window_end"] < science_end_ts]

        # create boundary change rows for the starts of all time windows defined in the config file
        start_rows = pd.DataFrame(
            {
                CONSTANTS.CSV_VARS.EPOCH: filtered["window_start"].values,
                CONSTANTS.CSV_VARS.OFFSET_X: np.where(
                    filtered["nan_x"], float("nan"), 0.0
                ),
                CONSTANTS.CSV_VARS.OFFSET_Y: np.where(
                    filtered["nan_y"], float("nan"), 0.0
                ),
                CONSTANTS.CSV_VARS.OFFSET_Z: np.where(
                    filtered["nan_z"], float("nan"), 0.0
                ),
                CONSTANTS.CSV_VARS.TIMEDELTA: np.zeros(len(filtered)),
                CONSTANTS.CSV_VARS.QUALITY_FLAG: filtered["quality_flag"].values,
                CONSTANTS.CSV_VARS.QUALITY_BITMASK: filtered["quality_bitmask"].values,
            }
        )
        # create boundary change rows for the ends of all time windows defined in the config file to reset values back to their previous values.
        # If the window ends after the end of the day then there will be zero end_rows because we are reading from end_filtered
        end_rows = pd.DataFrame(
            {
                CONSTANTS.CSV_VARS.EPOCH: end_filtered["window_end"].values,
                CONSTANTS.CSV_VARS.OFFSET_X: np.zeros(len(end_filtered)),
                CONSTANTS.CSV_VARS.OFFSET_Y: np.zeros(len(end_filtered)),
                CONSTANTS.CSV_VARS.OFFSET_Z: np.zeros(len(end_filtered)),
                CONSTANTS.CSV_VARS.TIMEDELTA: np.zeros(len(end_filtered)),
                CONSTANTS.CSV_VARS.QUALITY_FLAG: [pd.NA] * len(end_filtered),
                CONSTANTS.CSV_VARS.QUALITY_BITMASK: [pd.NA] * len(end_filtered),
            }
        )

        df = (
            pd.concat([start_rows, end_rows])
            .sort_values(CONSTANTS.CSV_VARS.EPOCH)
            .reset_index(drop=True)
            .astype(schema)
        )

        validity = (
            Validity(
                start=df[CONSTANTS.CSV_VARS.EPOCH].iloc[0],
                end=df[CONSTANTS.CSV_VARS.EPOCH].iloc[-1],
            )
            if not df.empty
            else Validity(start=science_start, end=science_end)
        )

        calfile = self.work_folder / cal_handler.get_filename()
        data_handler = cal_handler.get_equivalent_data_handler()
        datafile = self.work_folder / data_handler.get_filename()

        layer = CalibrationLayer(
            id="",
            mission=Mission.IMAP,
            validity=validity,
            sensor=self.calibration_job_parameters.sensor,
            version=cal_handler.version,
            metadata=CalibrationMetadata(
                dependencies=[],
                science=[],
                creation_timestamp=np.datetime64("now"),
                data_filename=Path(data_handler.get_filename()),
                content_date=np.datetime64(date),
            ),
            value_type=ValueType.BOUNDARY_CHANGES_ONLY,
            method=CalibrationMethod.SET_QUALITY_AND_NAN,
        )
        layer._contents = df

        layer.writeToFile(calfile)

        if not calfile.exists():
            raise FileNotFoundError(f"Calibration file {calfile} was not created.")
        if not datafile.exists():
            raise FileNotFoundError(f"Data file {datafile} was not created.")

        def raise_if_resequenced():
            raise ValueError(
                f"Calibration file {calfile} and data file {datafile} may not be resequenced due to their interdependence. If you need to resequence, please delete these files and re-run the calibration."
            )

        cal_handler.register_callback_on_resequencing(raise_if_resequenced)
        data_handler.register_callback_on_resequencing(raise_if_resequenced)

        return calfile, datafile

    def _get_science_time_range(
        self, science_file: Path | None
    ) -> tuple[np.datetime64, np.datetime64]:
        """Get the first and last timestamps from the science data file.

        Falls back to logical day boundaries if no science file is available.
        """
        date = self.calibration_job_parameters.date
        day_start = datetime(date.year, date.month, date.day)
        day_end = day_start + timedelta(days=1)

        if science_file is None or not Path(science_file).exists():
            logger.warning(
                "No science file available, using logical day boundaries for change points"
            )
            return np.datetime64(day_start), np.datetime64(day_end)

        science_layer = ScienceLayer.from_file(Path(science_file), load_contents=True)
        if science_layer._contents is None:
            return np.datetime64(day_start), np.datetime64(day_end)

        return np.datetime64(
            science_layer._contents[CONSTANTS.CSV_VARS.EPOCH].iloc[0]
        ), np.datetime64(science_layer._contents[CONSTANTS.CSV_VARS.EPOCH].iloc[-1])
