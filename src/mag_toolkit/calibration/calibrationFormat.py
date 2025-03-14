import os
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel


class CalibrationMethod(Enum):
    KEPKO = "Kepko"
    LEINWEBER = "Leinweber"
    IMAPLO_PIVOT = "IMAP-Lo Pivot Platform Interference"


class Sensor(Enum):
    MAGO = "MAGo"
    MAGI = "MAGi"


class CalibrationLayer(BaseModel):
    timestamps: list[datetime]
    offsets: list[list[float]]
    sensor: Sensor
    validity_start_time: datetime
    validity_end_time: datetime
    creation_timestamp: datetime
    dependencies: list[str]
    science: list[str]
    method: CalibrationMethod
    comment: Optional[str] = None
    version: int

    @classmethod
    def from_file(cls, path: Path):
        with open(path) as fid:
            as_dict = yaml.safe_load(fid)
        model = cls(**as_dict)
        return model

    def getWriteable(self):
        json = self.model_dump_json()

        return json

    def writeToFile(self, filepath: Path, createDirectory=False):
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


class ScienceLayerZero(CalibrationLayer):
    range: list[int]
    science_file: str
