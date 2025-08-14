import logging
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from cdflib.xarray import cdf_to_xarray, xarray_to_cdf

from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
    CalibrationMethod,
    CalibrationValue,
    ValueType,
)
from mag_toolkit.calibration.Layer import Layer

logger = logging.getLogger(__name__)


class CalibrationLayer(Layer):
    method: CalibrationMethod
    value_type: ValueType
    values: list[CalibrationValue]

    def as_df(self) -> pd.DataFrame:
        """
        Convert the calibration layer to a pandas DataFrame.
        """
        data = {
            "epoch": [value.time for value in self.values],
            "value": [value.value for value in self.values],
            "timedelta": [value.timedelta for value in self.values],
            "quality_flag": [value.quality_flag for value in self.values],
            "quality_bitmask": [value.quality_bitmask for value in self.values],
        }
        return pd.DataFrame(data)

    def _write_to_csv(self, filepath: Path, createDirectory=False):
        raise NotImplementedError(
            "CSV output not implemented for CalibrationLayer. Use CDF or JSON instead."
        )

    def _write_to_cdf(self, filepath: Path, createDirectory=False) -> Path:
        skeleton_cdf = cdf_to_xarray(
            str(CONSTANTS.OFFSET_SKELETON_CDF), to_datetime=False
        )
        epoch_values = [value.time for value in self.values]

        offsets_values = np.nan_to_num(
            [cal_value.value for cal_value in self.values],
            nan=CONSTANTS.CDF_FLOAT_FILLVAL,
        )

        epoch_data = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=epoch_values,
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.EPOCH].attrs,
        )
        offsets_data = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH, CONSTANTS.CDF_COORDS.AXIS],
            data=offsets_values,
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.OFFSETS].attrs,
        )
        timedelta_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=[cal_value.timedelta for cal_value in self.values],
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.TIMEDELTAS].attrs,
        )
        qf_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=[cal_value.quality_flag for cal_value in self.values],
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.QUALITY_FLAG].attrs,
        )
        qb_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=[cal_value.quality_bitmask for cal_value in self.values],
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.QUALITY_BITMASK].attrs,
        )
        offsets_dataset = xr.Dataset(
            data_vars={
                CONSTANTS.CDF_VARS.EPOCH: epoch_data,
                CONSTANTS.CDF_VARS.OFFSETS: offsets_data,
                CONSTANTS.CDF_VARS.TIMEDELTAS: timedelta_var,
                CONSTANTS.CDF_VARS.QUALITY_FLAG: qf_var,
                CONSTANTS.CDF_VARS.QUALITY_BITMASK: qb_var,
                CONSTANTS.CDF_VARS.VALIDITY_START_DATETIME: self.validity.start,
                CONSTANTS.CDF_VARS.VALIDITY_END_DATETIME: self.validity.end,
            },
            coords={
                CONSTANTS.CDF_COORDS.AXIS: [
                    CONSTANTS.CDF_COORDS.X,
                    CONSTANTS.CDF_COORDS.Y,
                    CONSTANTS.CDF_COORDS.Z,
                ]
            },
            attrs=skeleton_cdf.attrs,
        )

        offsets_dataset.attrs[CONSTANTS.CDF_ATTRS.GENERATION_DATE] = str(
            np.datetime64("now")
        )
        offsets_dataset.attrs[CONSTANTS.CDF_ATTRS.DATA_VERSION] = self.version

        xarray_to_cdf(offsets_dataset, str(filepath), istp=False)

        return filepath
