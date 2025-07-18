from pathlib import Path

import numpy as np
import xarray as xr
from cdflib.xarray import cdf_to_xarray, xarray_to_cdf

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
        skeleton_cdf = cdf_to_xarray(str(OFFSET_SKELETON_CDF), to_datetime=False)
        epoch_values = [value.time for value in self.values]

        offsets_values = np.nan_to_num(
            [cal_value.value for cal_value in self.values],
            nan=CDF_FLOAT_FILLVAL,
        )
        print(f"The size of offsets values is {offsets_values.shape}")
        epoch_data = xr.Variable(
            dims=["epoch"],
            data=epoch_values,
            attrs=skeleton_cdf["epoch"].attrs,
        )
        offsets_data = xr.Variable(
            dims=["epoch", "axis"],
            data=offsets_values,
            attrs=skeleton_cdf["offsets"].attrs,
        )
        timedelta_var = xr.Variable(  # noqa: F841
            dims=["epoch"],
            data=[cal_value.timedelta for cal_value in self.values],
            attrs=skeleton_cdf["timedeltas"].attrs,
        )
        qf_var = xr.Variable(  # noqa: F841
            dims=["epoch"],
            data=[cal_value.quality_flag for cal_value in self.values],
            attrs=skeleton_cdf["quality_flag"].attrs,
        )
        qb_var = xr.Variable(  # noqa: F841
            dims=["epoch"],
            data=[cal_value.quality_bitmask for cal_value in self.values],
            attrs=skeleton_cdf["quality_bitmask"].attrs,
        )
        offsets_dataset = xr.Dataset(
            data_vars={
                "epoch": epoch_data,
                "offsets": offsets_data,
            },
            coords={"axis": ["x", "y", "z"]},
            attrs=skeleton_cdf.attrs,
        )
        """
        "timedeltas": timedelta_var,
                "quality_flag": qf_var,
                "quality_bitmask": qb_var,
                "valid_start_datetime": self.validity.start,
                "valid_end_datetime": self.validity.end,
        """

        # offsets_dataset.attrs["Generation_date"] = np.datetime64("now")
        # offsets_dataset.attrs["Data_version"] = self.version

        xarray_to_cdf(offsets_dataset, str(filepath), istp=False)

        return filepath
