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
    Mission,
    ScienceValue,
    Sensor,
    Validity,
    ValueType,
)
from mag_toolkit.calibration.Layer import Layer

logger = logging.getLogger(__name__)


class ScienceLayer(Layer):
    science_file: str
    value_type: ValueType
    values: list[ScienceValue]

    def as_df(self) -> pd.DataFrame:
        """
        Convert the science layer to a pandas DataFrame.
        """
        data = {
            "epoch": [science.time for science in self.values],
            "x": [science.value[0] for science in self.values],
            "y": [science.value[1] for science in self.values],
            "z": [science.value[2] for science in self.values],
            "range": [science.range for science in self.values],
            "magnitude": [science.magnitude for science in self.values],
            "quality_flag": [science.quality_flag for science in self.values],
            "quality_bitmask": [science.quality_bitmask for science in self.values],
        }
        return pd.DataFrame(data)

    def _write_to_cdf(self, filepath: Path, createDirectory=False):
        l2_skeleton = cdf_to_xarray(str(CONSTANTS.L2_SKELETON_CDF), to_datetime=False)
        vectors_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH, CONSTANTS.CDF_COORDS.DIRECTION],
            data=[science.value for science in self.values],
            attrs=l2_skeleton[CONSTANTS.CDF_VARS.VECTORS].attrs,
        )
        epoch_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=[science.time for science in self.values],
            attrs=l2_skeleton[CONSTANTS.CDF_VARS.EPOCH].attrs,
        )
        magnitude_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=[science.magnitude for science in self.values],
            attrs=l2_skeleton[CONSTANTS.CDF_VARS.MAGNITUDE].attrs,
        )
        qf_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=[science.quality_flag for science in self.values],
            attrs=l2_skeleton[CONSTANTS.CDF_VARS.QUALITY_FLAGS].attrs,
        )
        qb_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=[science.quality_bitmask for science in self.values],
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
        epoch = [science.time for science in self.values]
        x = [science.value[0] for science in self.values]
        y = [science.value[1] for science in self.values]
        z = [science.value[2] for science in self.values]
        magnitude = [science.magnitude for science in self.values]
        range = [science.range for science in self.values]
        quality_flags = [science.quality_flag for science in self.values]
        quality_bitmask = [science.quality_bitmask for science in self.values]

        df = pd.DataFrame(
            {
                CONSTANTS.CSV_VARS.EPOCH: epoch,
                CONSTANTS.CSV_VARS.X: x,
                CONSTANTS.CSV_VARS.Y: y,
                CONSTANTS.CSV_VARS.Z: z,
                CONSTANTS.CSV_VARS.MAGNITUDE: magnitude,
                CONSTANTS.CSV_VARS.RANGE: range,
                CONSTANTS.CSV_VARS.QUALITY_FLAGS: quality_flags,
                CONSTANTS.CSV_VARS.QUALITY_BITMASK: quality_bitmask,
            }
        )
        # Before writing values, transofrm NaNs into CDF fill vals
        df.fillna(
            value={
                CONSTANTS.CSV_VARS.X: CONSTANTS.CDF_FLOAT_FILLVAL,
                CONSTANTS.CSV_VARS.Y: CONSTANTS.CDF_FLOAT_FILLVAL,
                CONSTANTS.CSV_VARS.Z: CONSTANTS.CDF_FLOAT_FILLVAL,
                CONSTANTS.CSV_VARS.MAGNITUDE: CONSTANTS.CDF_FLOAT_FILLVAL,
            },
            inplace=True,
        )
        df.to_csv(filepath)
        return filepath

    @classmethod
    def from_file(cls, path: Path):
        if path.suffix == ".cdf":
            return cls._from_cdf(path)
        elif path.suffix == ".csv":
            return cls._from_csv(path)
        else:
            return super().from_file(path)

    @classmethod
    def _from_csv(cls, path: Path):
        df = pd.read_csv(path)
        if df.empty:
            raise ValueError("CSV file is empty or does not contain valid data")

        epoch = df["t"].to_numpy(dtype=np.datetime64)
        x = df[CONSTANTS.CSV_VARS.X].to_numpy()
        y = df[CONSTANTS.CSV_VARS.Y].to_numpy()
        z = df[CONSTANTS.CSV_VARS.Z].to_numpy()
        range = df[CONSTANTS.CSV_VARS.RANGE].to_numpy()
        validity = Validity(start=epoch[0], end=epoch[-1])

        values = [
            ScienceValue(
                time=epoch_val,
                value=[x_val, y_val, z_val],
                range=range_val,
            )
            for epoch_val, x_val, y_val, z_val, range_val in zip(epoch, x, y, z, range)
        ]

        return cls(
            id="",
            mission=Mission.IMAP,
            validity=validity,
            sensor=Sensor.MAGO,
            version=0,
            metadata=CalibrationMetadata(
                dependencies=[],
                science=[],
                creation_timestamp=np.datetime64("now"),
            ),
            value_type=ValueType.VECTOR,
            science_file=str(path),
            values=values,
        )

    def calculate_magnitudes(self):
        for i, datapoint in enumerate(self.values):
            magnitude = np.linalg.norm(datapoint.value)
            self.values[i].magnitude = float(magnitude)
        return self

    @classmethod
    def _from_cdf(cls, path: Path):
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
            creation_timestamp=np.datetime64("now"),
        )

        values = [
            ScienceValue(
                time=epoch_val,
                value=datapoint[0:3],
                range=datapoint[3],
            )
            for raw_epoch_val, epoch_val, datapoint in zip(raw_epoch, epoch, data)
        ]

        return cls(
            id=dataset.attrs[CONSTANTS.CDF_ATTRS.LOGICAL_FILE_ID][0],
            mission=dataset.attrs[CONSTANTS.CDF_ATTRS.MISSION_GROUP][0],
            validity=validity,
            sensor=sensor,
            version=version,
            metadata=metadata,
            value_type=ValueType.VECTOR,
            science_file=str(path),
            values=values,
        )
