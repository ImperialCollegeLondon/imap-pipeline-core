import logging
from copy import deepcopy
from pathlib import Path

import cdflib as lib
import numpy as np
import pandas as pd
import xarray as xr
from cdflib.xarray import cdf_to_xarray, xarray_to_cdf

from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
    CalibrationMethod,
    ValueType,
)
from mag_toolkit.calibration.Layer import Layer

logger = logging.getLogger(__name__)


class CalibrationLayer(Layer):
    method: CalibrationMethod
    value_type: ValueType

    def _write_to_csv(self, filepath: Path, createDirectory=False):
        raise NotImplementedError(
            "CSV output not implemented for CalibrationLayer. Use CDF or JSON instead."
        )

    def compatible(self, other: Layer) -> bool:
        """Check if another calibration layer is time compatible with this one."""
        self.load_contents()
        other.load_contents()

        if self._contents is None or other._contents is None:
            raise ValueError("One of the layers has no data.")

        return all(
            self._contents[CONSTANTS.CSV_VARS.EPOCH]
            == other._contents[CONSTANTS.CSV_VARS.EPOCH]
        )

    def _convert_to_raw_epoch(self):
        if self._contents is None:
            raise ValueError("No contents loaded to convert.")

        if CONSTANTS.CSV_VARS.RAW_EPOCH in self._contents.columns:
            logger.debug("Raw epoch column already exists, skipping conversion.")
            return

        logger.debug("Converting epoch values to raw epoch format.")
        string_conversion = self._contents
        self._contents[CONSTANTS.CSV_VARS.RAW_EPOCH] = lib.cdfepoch.parse(
            string_conversion[CONSTANTS.CSV_VARS.EPOCH]
            .astype(str)
            .str.replace(" ", "T")
            .tolist()
        )

    def _write_to_cdf(self, filepath: Path, createDirectory=False) -> Path:
        logger.info("Writing calibration layer to CDF file.")
        skeleton_cdf = cdf_to_xarray(
            str(CONSTANTS.OFFSET_SKELETON_CDF), to_datetime=False
        )

        if self._contents is None:
            if self._data_path is None:
                raise ValueError("Calibration layer has no associated path for data.")
            self._contents = self._values_from_csv(self._data_path)

        logger.debug("Converting epoch values to raw epoch format for CDF.")
        self._convert_to_raw_epoch()

        offsets_values = np.nan_to_num(
            self._contents[
                [
                    CONSTANTS.CSV_VARS.OFFSET_X,
                    CONSTANTS.CSV_VARS.OFFSET_Y,
                    CONSTANTS.CSV_VARS.OFFSET_Z,
                ]
            ],
            nan=CONSTANTS.CDF_FLOAT_FILLVAL,
        )

        epoch_data = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=self._contents[CONSTANTS.CSV_VARS.RAW_EPOCH],
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.EPOCH].attrs,
        )
        offsets_data = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH, CONSTANTS.CDF_COORDS.AXIS],
            data=offsets_values,
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.OFFSETS].attrs,
        )
        offsets_data.attrs["DEPEND_0"] = CONSTANTS.CDF_VARS.EPOCH
        timedelta_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=self._contents[CONSTANTS.CSV_VARS.TIMEDELTA],
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.TIMEDELTAS].attrs,
        )
        timedelta_var.attrs["DEPEND_0"] = CONSTANTS.CDF_VARS.EPOCH
        qf_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=self._contents[CONSTANTS.CSV_VARS.QUALITY_FLAG],
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.QUALITY_FLAG].attrs,
        )
        qf_var.attrs["DEPEND_0"] = CONSTANTS.CDF_VARS.EPOCH
        qb_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=self._contents[CONSTANTS.CSV_VARS.QUALITY_BITMASK],
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.QUALITY_BITMASK].attrs,
        )
        qb_var.attrs["DEPEND_0"] = CONSTANTS.CDF_VARS.EPOCH
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
        )  # type: ignore

        offsets_dataset.attrs[CONSTANTS.CDF_ATTRS.GENERATION_DATE] = str(
            np.datetime64("now")
        )
        offsets_dataset.attrs[CONSTANTS.CDF_ATTRS.DATA_VERSION] = self.version

        offsets_dataset.attrs["Parents"] = deepcopy(self.metadata.dependencies)

        xarray_to_cdf(offsets_dataset, str(filepath), istp=True, compression=6)

        return filepath

    def _load_data_file(self, path: Path, generator=True) -> "CalibrationLayer":
        logger.debug(f"Loading calibration layer data from {path!s}.")
        if self._contents is not None:
            logger.warning(
                f"Existing calibration values will be overwritten with data in {path!s}."
            )

        self._contents = self._values_from_csv(path)
        return self

    @classmethod
    def _values_from_csv(cls, path: Path) -> pd.DataFrame:
        df = pd.read_csv(
            path, parse_dates=[CONSTANTS.CSV_VARS.EPOCH], float_precision="round_trip"
        )
        if df.empty:
            raise ValueError("CSV file is empty or does not contain valid data")
        return df
