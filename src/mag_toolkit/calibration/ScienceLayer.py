import logging
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from cdflib import cdfepoch
from cdflib.xarray import cdf_to_xarray, xarray_to_cdf
from pydantic import TypeAdapter

from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
    CalibrationMetadata,
    Sensor,
    Validity,
    ValueType,
)
from mag_toolkit.calibration.Layer import Layer

logger = logging.getLogger(__name__)


class ScienceLayer(Layer):
    science_file: str
    value_type: ValueType

    def _write_to_cdf(self, filepath: Path, createDirectory=False):
        l2_skeleton = cdf_to_xarray(str(CONSTANTS.L2_SKELETON_CDF), to_datetime=False)
        if self._contents is None:
            raise ValueError("No science data available to write to CDF.")
        vectors_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH, CONSTANTS.CDF_COORDS.DIRECTION],
            data=self._contents[
                [CONSTANTS.CSV_VARS.X, CONSTANTS.CSV_VARS.Y, CONSTANTS.CSV_VARS.Z]
            ],
            attrs=l2_skeleton[CONSTANTS.CDF_VARS.VECTORS].attrs,
        )
        epoch_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=self._contents[CONSTANTS.CSV_VARS.EPOCH],
            attrs=l2_skeleton[CONSTANTS.CDF_VARS.EPOCH].attrs,
        )
        magnitude_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=self._contents[CONSTANTS.CSV_VARS.MAGNITUDE],
            attrs=l2_skeleton[CONSTANTS.CDF_VARS.MAGNITUDE].attrs,
        )
        qf_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=self._contents[CONSTANTS.CSV_VARS.QUALITY_FLAG],
            attrs=l2_skeleton[CONSTANTS.CDF_VARS.QUALITY_FLAGS].attrs,
        )
        qb_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=self._contents[CONSTANTS.CSV_VARS.QUALITY_BITMASK],
            attrs=l2_skeleton[CONSTANTS.CDF_VARS.QUALITY_BITMASK].attrs,
        )
        del l2_skeleton.coords[CONSTANTS.CDF_VARS.EPOCH]
        l2_dataset = xr.Dataset(
            data_vars={
                CONSTANTS.CDF_VARS.EPOCH: epoch_var,
                CONSTANTS.CDF_VARS.VECTORS: vectors_var,
                CONSTANTS.CDF_VARS.MAGNITUDE: magnitude_var,
                CONSTANTS.CDF_VARS.QUALITY_FLAGS: qf_var,
                CONSTANTS.CDF_VARS.QUALITY_BITMASK: qb_var,
            },
            attrs=l2_skeleton.attrs,
            coords=l2_skeleton.coords,
        )

        xarray_to_cdf(l2_dataset, str(filepath), istp=False)
        return filepath

    def _write_to_csv(self, filepath: Path, createDirectory=False):
        # Before writing values, transofrm NaNs into CDF fill vals
        if self._contents is None:
            raise ValueError("No science data available to write to CSV.")
        self._contents.fillna(
            value={
                CONSTANTS.CSV_VARS.X: CONSTANTS.CDF_FLOAT_FILLVAL,
                CONSTANTS.CSV_VARS.Y: CONSTANTS.CDF_FLOAT_FILLVAL,
                CONSTANTS.CSV_VARS.Z: CONSTANTS.CDF_FLOAT_FILLVAL,
                CONSTANTS.CSV_VARS.MAGNITUDE: CONSTANTS.CDF_FLOAT_FILLVAL,
            },
            inplace=True,
        )
        self._contents.to_csv(filepath)
        return filepath

    @classmethod
    def from_file(cls, path: Path, load_contents=False) -> "ScienceLayer":
        if path.suffix == ".cdf":
            return cls._from_cdf(path, load_contents=load_contents)
        else:
            return super().from_file(path)

    def _load_data_file(self, path: Path) -> "ScienceLayer":
        if self._contents is not None:
            logger.warning(
                f"Existing science values will be overwritten with data in {path!s}."
            )

        dataset = cdf_to_xarray(str(path), to_datetime=False)

        data = dataset[CONSTANTS.CDF_VARS.VECTORS].values
        raw_epoch = dataset[CONSTANTS.CDF_VARS.EPOCH].values
        epoch = cdfepoch.to_datetime(raw_epoch)

        if data is None or epoch is None:
            raise ValueError("CDF does not contain valid data")

        self._contents = pd.DataFrame(
            {
                CONSTANTS.CSV_VARS.EPOCH: epoch,
                CONSTANTS.CSV_VARS.X: data[:, 0],
                CONSTANTS.CSV_VARS.Y: data[:, 1],
                CONSTANTS.CSV_VARS.Z: data[:, 2],
                CONSTANTS.CSV_VARS.RANGE: data[:, 3],
            }
        )

        return self

    def calculate_magnitudes(self):
        if self._contents is None:
            raise ValueError("Science layer contents not loaded")
        self._contents[CONSTANTS.CSV_VARS.MAGNITUDE] = np.linalg.norm(
            self._contents[
                [CONSTANTS.CSV_VARS.X, CONSTANTS.CSV_VARS.Y, CONSTANTS.CSV_VARS.Z]
            ]
        )
        return self

    def _set_data_path(self, path: Path) -> "ScienceLayer":
        self._data_path = path
        return self

    def _set_contents(self, contents: pd.DataFrame) -> "ScienceLayer":
        self._contents = contents
        return self

    @classmethod
    def _from_cdf(cls, path: Path, load_contents=False):
        dataset = cdf_to_xarray(str(path), to_datetime=False)

        data = dataset[CONSTANTS.CDF_VARS.VECTORS].values
        raw_epoch = dataset[CONSTANTS.CDF_VARS.EPOCH].values
        epoch = cdfepoch.to_datetime(raw_epoch)

        if data is None or epoch is None:
            raise ValueError("CDF does not contain valid data")

        validity = Validity(start=epoch[0], end=epoch[-1])

        sensor = (
            Sensor.MAGO
            if TypeAdapter(bool).validate_python(
                dataset.attrs[CONSTANTS.CDF_ATTRS.IS_MAGO][0]
            )
            else Sensor.MAGI
        )

        version = int(dataset.attrs[CONSTANTS.CDF_ATTRS.DATA_VERSION][0][1:])

        metadata = CalibrationMetadata(
            dependencies=[],
            science=[],
            data_filename=path,
            creation_timestamp=np.datetime64("now"),
        )

        contents = None
        if load_contents:
            contents = pd.DataFrame(
                {
                    CONSTANTS.CSV_VARS.EPOCH: epoch,
                    CONSTANTS.CSV_VARS.X: data[:, 0],
                    CONSTANTS.CSV_VARS.Y: data[:, 1],
                    CONSTANTS.CSV_VARS.Z: data[:, 2],
                    CONSTANTS.CSV_VARS.RANGE: data[:, 3],
                }
            )

        science_layer = cls(
            id=dataset.attrs[CONSTANTS.CDF_ATTRS.LOGICAL_FILE_ID][0],
            mission=dataset.attrs[CONSTANTS.CDF_ATTRS.MISSION_GROUP][0],
            validity=validity,
            sensor=sensor,
            version=version,
            metadata=metadata,
            value_type=ValueType.VECTOR,
            science_file=str(path),
        )
        science_layer._set_data_path(path)
        if load_contents and contents is not None:
            science_layer._set_contents(contents)

        return science_layer
