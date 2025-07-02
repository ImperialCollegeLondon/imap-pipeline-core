from datetime import datetime
from pathlib import Path

import numpy as np
from spacepy import pycdf

from mag_toolkit.calibration.CalibrationDefinitions import (
    CDF_FLOAT_FILLVAL,
    CalibrationMethod,
    CalibrationValue,
    ValueType,
)
from mag_toolkit.calibration.Layer import Layer


class CalibrationLayer(Layer):
    method: CalibrationMethod
    value_type: ValueType
    values: list[CalibrationValue]

    def _write_to_csv(self, filepath: Path, createDirectory=False):
        raise NotImplementedError(
            "CSV output not implemented for CalibrationLayer. Use CDF or JSON instead."
        )

    def _write_to_cdf(self, filepath: Path, createDirectory=False) -> Path:
        OFFSET_SKELETON_CDF = "resource/l2_offset_skeleton.cdf"
        with pycdf.CDF(str(filepath), OFFSET_SKELETON_CDF) as offset_cdf:
            offset_cdf["epoch"] = [value.time for value in self.values]
            offset_cdf["offsets"][...] = np.nan_to_num(
                [cal_value.value for cal_value in self.values], nan=CDF_FLOAT_FILLVAL
            )
            offset_cdf["timedeltas"] = [
                cal_value.timedelta for cal_value in self.values
            ]
            offset_cdf["quality_flag"] = [
                cal_value.quality_flag for cal_value in self.values
            ]
            offset_cdf["quality_bitmask"] = [
                cal_value.quality_bitmask for cal_value in self.values
            ]
            offset_cdf["valid_start_datetime"] = self.validity.start
            offset_cdf["valid_end_datetime"] = self.validity.end

            offset_cdf.attrs["Generation_date"] = datetime.now()
            offset_cdf.attrs["Data_version"] = self.version

        return filepath
