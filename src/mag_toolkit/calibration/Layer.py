import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel

from mag_toolkit.calibration.CalibrationDefinitions import (
    CalibrationMetadata,
    Mission,
    Sensor,
    Validity,
)


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

        with open(filepath, "w+") as f:
            f.write(json)

        return filepath

    def writeToFile(self, filepath: Path, createDirectory=False) -> Path:
        if filepath.suffix == ".cdf":
            return self._write_to_cdf(filepath, createDirectory=createDirectory)
        elif filepath.suffix == ".csv":
            return self._write_to_csv(filepath, createDirectory=createDirectory)
        else:
            return self._write_to_json(filepath, createDirectory=createDirectory)
