import logging
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from cdflib import cdfepoch
from cdflib.xarray import cdf_to_xarray, xarray_to_cdf

from mag_toolkit.calibration.CalibrationDefinitions import (
    CDF_FLOAT_FILLVAL,
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

    def _write_to_cdf(self, filepath: Path, createDirectory=False):
        L2_SKELETON_CDF = "resource/l2_dsrf_skeleton.cdf"
        l2_skeleton = cdf_to_xarray(str(L2_SKELETON_CDF), to_datetime=False)
        vectors_var = xr.Variable(
            dims=["epoch", "direction"],
            data=[science.value for science in self.values],
            attrs=l2_skeleton["vectors"].attrs,
        )
        epoch_var = xr.Variable(
            dims=["epoch"],
            data=[science.time for science in self.values],
            attrs=l2_skeleton["epoch"].attrs,
        )
        magnitude_var = xr.Variable(
            dims=["epoch"],
            data=[science.magnitude for science in self.values],
            attrs=l2_skeleton["magnitude"].attrs,
        )
        qf_var = xr.Variable(
            dims=["epoch"],
            data=[science.quality_flag for science in self.values],
            attrs=l2_skeleton["quality_flags"].attrs,
        )
        qb_var = xr.Variable(
            dims=["epoch"],
            data=[science.quality_bitmask for science in self.values],
            attrs=l2_skeleton["quality_bitmask"].attrs,
        )
        del l2_skeleton.coords["epoch"]
        l2_dataset = xr.Dataset(
            data_vars={
                "epoch": epoch_var,
                "vectors": vectors_var,
                "magnitude": magnitude_var,
                "quality_flags": qf_var,
                "quality_bitmask": qb_var,
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
                "epoch": epoch,
                "x": x,
                "y": y,
                "z": z,
                "magnitude": magnitude,
                "range": range,
                "quality_flags": quality_flags,
                "quality_bitmask": quality_bitmask,
            }
        )
        # Before writing values, transofrm NaNs into CDF fill vals
        df.fillna(
            value={
                "x": CDF_FLOAT_FILLVAL,
                "y": CDF_FLOAT_FILLVAL,
                "z": CDF_FLOAT_FILLVAL,
                "magnitude": CDF_FLOAT_FILLVAL,
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
        x = df["x"].to_numpy()
        y = df["y"].to_numpy()
        z = df["z"].to_numpy()
        range = df["range"].to_numpy()
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

    @classmethod
    def _from_cdf(cls, path: Path):
        dataset = cdf_to_xarray(str(path), to_datetime=False)

        # cdf_loaded = pycdf.CDF(str(path))

        data = dataset["vectors"].values
        raw_epoch = dataset["epoch"].values
        epoch = cdfepoch.to_datetime(raw_epoch)

        if data is None or epoch is None:
            raise ValueError("CDF does not contain valid data")

        validity = Validity(start=epoch[0], end=epoch[-1])

        sensor = Sensor.MAGO if dataset.attrs["is_mago"][0] == "True" else Sensor.MAGI

        version = int(dataset.attrs["Data_version"][0][1:])

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

        return ScienceLayer(
            id=dataset.attrs["Logical_file_id"][0],
            mission=dataset.attrs["Mission_group"][0],
            validity=validity,
            sensor=sensor,
            version=version,
            metadata=metadata,
            value_type=ValueType.VECTOR,
            science_file=str(path),
            values=values,
        )
