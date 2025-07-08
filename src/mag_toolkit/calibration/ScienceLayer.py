from datetime import datetime
from pathlib import Path

import pandas as pd
from spacepy import pycdf

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


class ScienceLayer(Layer):
    science_file: str
    value_type: ValueType
    values: list[ScienceValue]

    def _write_to_cdf(self, filepath: Path, createDirectory=False):
        L2_SKELETON_CDF = "resource/l2_dsrf_skeleton.cdf"
        epoch = [science.time for science in self.values]
        vecs = [science.value for science in self.values]
        magnitude = [science.magnitude for science in self.values]
        quality_flags = [science.quality_flag for science in self.values]
        quality_bitmask = [science.quality_bitmask for science in self.values]

        with pycdf.CDF(str(filepath), L2_SKELETON_CDF) as cdf:
            cdf["epoch"] = epoch
            cdf["vectors"][...] = vecs
            cdf["magnitude"] = magnitude
            cdf["quality_flags"] = quality_flags
            cdf["quality_bitmask"] = quality_bitmask
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

        epoch = df["t"].to_numpy()
        x = df["x"].to_numpy()
        y = df["y"].to_numpy()
        z = df["z"].to_numpy()
        range = df["range"].to_numpy()
        validity = Validity(start=epoch[0], end=epoch[-1])

        values = [
            ScienceValue(time=epoch_val, value=[x_val, y_val, z_val], range=range_val)
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
                creation_timestamp=datetime.now(),
            ),
            value_type=ValueType.VECTOR,
            science_file=str(path),
            values=values,
        )

    @classmethod
    def _from_cdf(cls, path: Path):
        cdf_loaded = pycdf.CDF(str(path))

        data = cdf_loaded["vectors"][...]
        epoch = cdf_loaded["epoch"][...]

        if data is None or epoch is None:
            raise ValueError("CDF does not contain valid data")

        validity = Validity(start=epoch[0], end=epoch[-1])

        sensor = Sensor.MAGO if cdf_loaded.attrs["is_mago"] else Sensor.MAGI

        version = int(cdf_loaded.attrs["Data_version"][0][1:])

        metadata = CalibrationMetadata(
            dependencies=[],
            science=[],
            creation_timestamp=datetime.now(),
        )

        values = [
            ScienceValue(time=epoch_val, value=datapoint[0:3], range=datapoint[3])
            for epoch_val, datapoint in zip(epoch, data)
        ]

        return ScienceLayer(
            id=cdf_loaded.attrs["Logical_file_id"][0],
            mission=cdf_loaded.attrs["Mission_group"][0],
            validity=validity,
            sensor=sensor,
            version=version,
            metadata=metadata,
            value_type=ValueType.VECTOR,
            science_file=str(path),
            values=values,
        )
