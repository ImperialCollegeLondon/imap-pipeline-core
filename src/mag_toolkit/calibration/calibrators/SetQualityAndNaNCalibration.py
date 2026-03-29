import logging
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from imap_mag.config.CalibrationConfig import CalibrationConfig
from imap_mag.io.file import CalibrationLayerPathHandler
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

from .CalibrationJob import CalibrationJob

logger = logging.getLogger(__name__)


class SetQualityAndNaNCalibrationJob(CalibrationJob):
    def __init__(
        self, calibration_job_parameters: CalibrationJobParameters, work_folder: Path
    ):
        super().__init__(calibration_job_parameters, work_folder)
        self.name = CalibrationMethod.SET_QUALITY_AND_NAN

    def _get_path_handlers(self, calibration_job_parameters: CalibrationJobParameters):
        return {}

    def needs_data_store(self):
        return False

    def run_calibration(
        self, cal_handler: CalibrationLayerPathHandler, config: CalibrationConfig
    ) -> tuple[Path, Path]:
        if config.set_quality_and_nan is None:
            raise ValueError(
                "SetQualityAndNaN calibration requires a set_quality_and_nan configuration "
                "with a csv_file path."
            )

        csv_path = Path(config.set_quality_and_nan.csv_file)
        if not csv_path.exists():
            raise FileNotFoundError(f"SetQualityAndNaN CSV file not found: {csv_path}")

        input_df = pd.read_csv(csv_path)
        input_df["start_date"] = pd.to_datetime(input_df["start_date"])
        input_df["end_date"] = pd.to_datetime(input_df["end_date"])

        for col in ["nan_x", "nan_y", "nan_z"]:
            input_df[col] = input_df[col].astype(bool)

        date = self.calibration_job_parameters.date
        change_points = self._generate_change_points(input_df, date)

        if not change_points:
            raise ValueError(
                f"No quality/NaN windows overlap with date {date.strftime('%Y-%m-%d')}"
            )

        change_points.sort(key=lambda p: p[CONSTANTS.CSV_VARS.EPOCH])
        df = pd.DataFrame(change_points)

        validity = Validity(
            start=df[CONSTANTS.CSV_VARS.EPOCH].iloc[0],
            end=df[CONSTANTS.CSV_VARS.EPOCH].iloc[-1],
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
            value_type=ValueType.INTERPOLATION_POINTS,
            method=CalibrationMethod.SET_QUALITY_AND_NAN,
        )
        layer._contents = df

        layer.writeToFile(calfile)

        if not calfile.exists():
            raise FileNotFoundError(f"Calibration file {calfile} was not created.")
        if not datafile.exists():
            raise FileNotFoundError(f"Data file {datafile} was not created.")

        return calfile, datafile

    def _generate_change_points(
        self, input_df: pd.DataFrame, date: datetime
    ) -> list[dict]:
        day_start = datetime(date.year, date.month, date.day)
        day_end = day_start + timedelta(days=1)

        change_points: list[dict] = []

        for _, row in input_df.iterrows():
            start = row["start_date"].to_pydatetime()
            end = row["end_date"].to_pydatetime()

            if start >= day_end or end <= day_start:
                continue

            window_start = max(start, day_start)
            window_end = min(end, day_end)

            offset_x = float("nan") if row["nan_x"] else 0.0
            offset_y = float("nan") if row["nan_y"] else 0.0
            offset_z = float("nan") if row["nan_z"] else 0.0

            change_points.append(
                {
                    CONSTANTS.CSV_VARS.EPOCH: pd.Timestamp(window_start),
                    CONSTANTS.CSV_VARS.OFFSET_X: offset_x,
                    CONSTANTS.CSV_VARS.OFFSET_Y: offset_y,
                    CONSTANTS.CSV_VARS.OFFSET_Z: offset_z,
                    CONSTANTS.CSV_VARS.TIMEDELTA: 0.0,
                    CONSTANTS.CSV_VARS.QUALITY_FLAG: int(row["quality_flag"]),
                    CONSTANTS.CSV_VARS.QUALITY_BITMASK: int(row["quality_bitmask"]),
                }
            )

            if window_end < day_end:
                change_points.append(
                    {
                        CONSTANTS.CSV_VARS.EPOCH: pd.Timestamp(window_end),
                        CONSTANTS.CSV_VARS.OFFSET_X: 0.0,
                        CONSTANTS.CSV_VARS.OFFSET_Y: 0.0,
                        CONSTANTS.CSV_VARS.OFFSET_Z: 0.0,
                        CONSTANTS.CSV_VARS.TIMEDELTA: 0.0,
                        CONSTANTS.CSV_VARS.QUALITY_FLAG: 0,
                        CONSTANTS.CSV_VARS.QUALITY_BITMASK: 0,
                    }
                )

        return change_points
