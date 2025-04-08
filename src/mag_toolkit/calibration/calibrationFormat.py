import os
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml
from pydantic import BaseModel
from spacepy import pycdf

from mag_toolkit.calibration.Calibrator import CalibrationMethod


class Sensor(str, Enum):
    MAGO = "MAGo"
    MAGI = "MAGi"


class Mission(str, Enum):
    IMAP = "IMAP"


class CalibrationMetadata(BaseModel):
    dependencies: list[str]
    science: list[str]
    creation_timestamp: datetime
    comment: Optional[str] = None


class Value(BaseModel, ABC):
    time: datetime
    value: list[float]


class CalibrationValue(Value):
    timedelta: float = 0
    quality_flag: int = 0
    quality_bitmask: int = 0


class ScienceValue(Value):
    range: int
    quality_flag: Optional[int] = 0
    quality_bitmask: Optional[int] = 0


class Validity(BaseModel):
    start: datetime
    end: datetime


class Layer(BaseModel, ABC):
    id: str
    mission: Mission
    validity: Validity
    sensor: Sensor
    version: int
    metadata: CalibrationMetadata
    rotation: Optional[list[list[list[float]]]] = None

    @classmethod
    def from_file(cls, path: Path):
        with open(path) as fid:
            as_dict = yaml.safe_load(fid)
        model = cls(**as_dict)
        return model

    @abstractmethod
    def _write_to_cdf(self, filepath: Path, createDirectory=False) -> Path: ...

    @abstractmethod
    def _write_to_csv(self, filepath: Path, createDirectory=False) -> Path: ...

    def getWriteable(self):
        json = self.model_dump_json()

        return json

    def _write_to_json(self, filepath: Path, createDirectory=False):
        json = self.model_dump_json()

        if createDirectory:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

        try:
            with open(filepath, "w+") as f:
                f.write(json)
        except Exception as e:
            print(e)
            print(f"Failed to write calibration to {filepath}")

        return filepath

    def writeToFile(self, filepath: Path, createDirectory=False) -> Path:
        if filepath.suffix == ".cdf":
            return self._write_to_cdf(filepath, createDirectory=createDirectory)
        elif filepath.suffix == ".csv":
            return self._write_to_csv(filepath, createDirectory=createDirectory)
        else:
            return self._write_to_json(filepath, createDirectory=createDirectory)


class CalibrationLayer(Layer):
    method: CalibrationMethod
    value_type: str
    values: list[CalibrationValue]

    def _write_to_csv(self, filepath: Path, createDirectory=False):
        return Path()

    def _write_to_cdf(self, filepath: Path, createDirectory=False) -> Path:
        OFFSET_SKELETON_CDF = "resource/l2_offset_skeleton.cdf"
        with pycdf.CDF(str(filepath), OFFSET_SKELETON_CDF) as offset_cdf:
            offset_cdf["epoch"] = [value.time for value in self.values]
            offset_cdf["offsets"][...] = [cal_value.value for cal_value in self.values]
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


class ScienceLayer(Layer):
    science_file: str
    value_type: str
    values: list[ScienceValue]

    def _write_to_cdf(self, filepath: Path, createDirectory=False):
        return Path()

    def _write_to_csv(self, filepath: Path, createDirectory=False):
        epoch = [science.time for science in self.values]
        x = [science.value[0] for science in self.values]
        y = [science.value[1] for science in self.values]
        z = [science.value[2] for science in self.values]
        range = [science.range for science in self.values]
        quality_flags = [science.quality_flag for science in self.values]
        quality_bitmask = [science.quality_bitmask for science in self.values]

        df = pd.DataFrame(
            {
                "epoch": epoch,
                "x": x,
                "y": y,
                "z": z,
                "range": range,
                "quality_flags": quality_flags,
                "quality_bitmask": quality_bitmask,
            }
        )
        df.to_csv(filepath)
        return filepath

    @classmethod
    def from_file(cls, path: Path):
        if path.suffix == ".cdf":
            return cls._from_cdf(path)
        else:
            return super().from_file(path)

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
            value_type="vector",
            science_file=str(path),
            values=values,
        )
