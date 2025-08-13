import logging
from copy import deepcopy
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from cdflib.xarray import cdf_to_xarray, xarray_to_cdf

from imap_mag.io.file import CalibrationMetadataPathHandler
from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
    CalibrationMetadata,
    CalibrationMethod,
    CalibrationValue,
    Mission,
    Sensor,
    Validity,
    ValueType,
)
from mag_toolkit.calibration.Layer import Layer

logger = logging.getLogger(__name__)


class CalibrationLayer(Layer):
    method: CalibrationMethod
    value_type: ValueType
    values: list[CalibrationValue] = []  # noqa: RUF012

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

    @classmethod
    def from_file(cls, path: Path) -> "CalibrationLayer":
        if path.suffix == ".csv":
            return cls._from_csv(path)
        else:
            return super().from_file(path)

    @classmethod
    def _load_data_file(cls, path: Path, existing_model) -> "CalibrationLayer":
        calibration_only_layer = cls.from_file(path)
        existing_model.values = deepcopy(calibration_only_layer.values)

        return existing_model

    @classmethod
    def _from_csv(cls, path: Path):
        df = pd.read_csv(path, float_precision="round_trip")
        if df.empty:
            raise ValueError("CSV file is empty or does not contain valid data")

        epoch = df[CONSTANTS.CSV_VARS.EPOCH].to_numpy(dtype=np.datetime64)
        x = df[CONSTANTS.CSV_VARS.OFFSET_X].to_numpy()
        y = df[CONSTANTS.CSV_VARS.OFFSET_Y].to_numpy()
        z = df[CONSTANTS.CSV_VARS.OFFSET_Z].to_numpy()
        timedelta = df[CONSTANTS.CSV_VARS.TIMEDELTA].to_numpy()
        quality_flag = df[CONSTANTS.CSV_VARS.QUALITY_FLAG].to_numpy()
        quality_bitmask = df[CONSTANTS.CSV_VARS.QUALITY_BITMASK].to_numpy()
        validity = Validity(start=epoch[0], end=epoch[-1])

        values = [
            CalibrationValue(
                time=epoch_val,
                value=[x_val, y_val, z_val],
                timedelta=delta_val,
                quality_flag=flag_val,
                quality_bitmask=bitmask_val,
            )
            for epoch_val, x_val, y_val, z_val, delta_val, flag_val, bitmask_val in zip(
                epoch, x, y, z, timedelta, quality_flag, quality_bitmask
            )
        ]

        calibration_metadata_handler = CalibrationMetadataPathHandler.from_filename(
            path
        )

        method: CalibrationMethod = (
            CalibrationMethod.from_string(
                calibration_metadata_handler.calibration_descriptor
            )
            if (
                calibration_metadata_handler
                and calibration_metadata_handler.calibration_descriptor
            )
            else CalibrationMethod.NOOP
        )

        return cls(
            id="",
            mission=Mission.IMAP,
            validity=validity,
            sensor=Sensor.MAGO,
            version=0,
            metadata=CalibrationMetadata(
                dependencies=[],
                science=[],
                data_filename=path,
                creation_timestamp=np.datetime64("now"),
            ),
            value_type=ValueType.VECTOR,
            method=method,
            values=values,
        )
