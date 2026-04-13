import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from imap_mag.config.CalibrationConfig import CalibrationConfig
from imap_mag.io.file import CalibrationLayerPathHandler, SciencePathHandler
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
        self, calibration_job_parameters: CalibrationJobParameters, work_folder: Path
    ):
        super().__init__(calibration_job_parameters, work_folder)
        self.name = CalibrationMethod.SET_QUALITY_AND_NAN
        self.required_files[self.science_file_key] = None

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

        csv_path = Path(config.set_quality_and_nan.csv_file)
        # check if it is an absolute path, if not assume it's relative to the current working directory
        if not csv_path.is_absolute() and self.data_store is not None:
            csv_path = Path(self.data_store) / csv_path

        if not csv_path.exists():
            raise FileNotFoundError(f"SetQualityAndNaN CSV file not found: {csv_path}")

        input_df = pd.read_csv(csv_path)
        input_df["start_date"] = pd.to_datetime(input_df["start_date"])
        input_df["end_date"] = pd.to_datetime(input_df["end_date"])

        for col in ["nan_x", "nan_y", "nan_z"]:
            input_df[col] = input_df[col].astype(bool)

        date = self.calibration_job_parameters.date

        # Get actual science data time range from the science file
        science_file = self.required_files.get(self.science_file_key)
        science_start, science_end = self._get_science_time_range(science_file)

        change_points = self._generate_change_points(
            input_df, date, science_start, science_end
        )

        change_points.sort(key=lambda p: p[CONSTANTS.CSV_VARS.EPOCH])
        if change_points:
            df = pd.DataFrame(change_points)
        else:
            df = pd.DataFrame(
                columns=[
                    CONSTANTS.CSV_VARS.EPOCH,
                    CONSTANTS.CSV_VARS.OFFSET_X,
                    CONSTANTS.CSV_VARS.OFFSET_Y,
                    CONSTANTS.CSV_VARS.OFFSET_Z,
                    CONSTANTS.CSV_VARS.TIMEDELTA,
                    CONSTANTS.CSV_VARS.QUALITY_FLAG,
                    CONSTANTS.CSV_VARS.QUALITY_BITMASK,
                ]
            )

        # ensure qualityflag is nullable integer
        df[CONSTANTS.CSV_VARS.QUALITY_FLAG] = df[
            CONSTANTS.CSV_VARS.QUALITY_FLAG
        ].astype(pd.Int64Dtype())
        # ensure quality bitmask is nullable integer
        df[CONSTANTS.CSV_VARS.QUALITY_BITMASK] = df[
            CONSTANTS.CSV_VARS.QUALITY_BITMASK
        ].astype(pd.Int64Dtype())

        validity = (
            Validity(
                start=df[CONSTANTS.CSV_VARS.EPOCH].iloc[0],
                end=df[CONSTANTS.CSV_VARS.EPOCH].iloc[-1],
            )
            if change_points
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

    def _generate_change_points(
        self,
        input_df: pd.DataFrame,
        date: datetime,
        science_start: np.datetime64,
        science_end: np.datetime64,
    ) -> list[dict]:
        day_start = np.datetime64(datetime(date.year, date.month, date.day))
        day_end = day_start + np.timedelta64(1, "D")

        change_points: list[dict] = []

        for _, row in input_df.iterrows():
            start = np.datetime64(row["start_date"])
            end = np.datetime64(row["end_date"])

            # Skip windows entirely outside the science data range
            if start > science_end or end < science_start:
                continue

            # Also skip if entirely outside the logical day
            if start >= day_end or end <= day_start:
                continue

            window_start = max(start, day_start)
            window_end = min(end, science_end)

            offset_x = float("nan") if row["nan_x"] else 0.0
            offset_y = float("nan") if row["nan_y"] else 0.0
            offset_z = float("nan") if row["nan_z"] else 0.0

            change_points.append(
                {
                    CONSTANTS.CSV_VARS.EPOCH: pd.Timestamp(
                        window_start,
                    ),
                    CONSTANTS.CSV_VARS.OFFSET_X: offset_x,
                    CONSTANTS.CSV_VARS.OFFSET_Y: offset_y,
                    CONSTANTS.CSV_VARS.OFFSET_Z: offset_z,
                    CONSTANTS.CSV_VARS.TIMEDELTA: 0.0,
                    CONSTANTS.CSV_VARS.QUALITY_FLAG: int(row["quality_flag"])
                    if pd.notna(row["quality_flag"])
                    else None,
                    CONSTANTS.CSV_VARS.QUALITY_BITMASK: int(row["quality_bitmask"])
                    if pd.notna(row["quality_bitmask"])
                    else None,
                }
            )

            # Only add end-of-window reset if the window ends within the science data range
            if window_end < science_end:
                change_points.append(
                    {
                        CONSTANTS.CSV_VARS.EPOCH: pd.Timestamp(window_end),
                        CONSTANTS.CSV_VARS.OFFSET_X: 0.0,
                        CONSTANTS.CSV_VARS.OFFSET_Y: 0.0,
                        CONSTANTS.CSV_VARS.OFFSET_Z: 0.0,
                        CONSTANTS.CSV_VARS.TIMEDELTA: 0.0,
                        CONSTANTS.CSV_VARS.QUALITY_FLAG: None,
                        CONSTANTS.CSV_VARS.QUALITY_BITMASK: None,
                    }
                )

        return change_points
