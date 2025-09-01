import logging
import typing
from abc import ABC, abstractmethod
from pathlib import Path

import yaml
from pydantic import BaseModel

from mag_toolkit.calibration.CalibrationDefinitions import (
    CalibrationMetadata,
    Mission,
    Sensor,
    Validity,
)

logger = logging.getLogger(__name__)

T = typing.TypeVar("T", bound="Layer")


class Layer(BaseModel, ABC):
    id: str
    mission: Mission
    validity: Validity
    sensor: Sensor
    version: int
    metadata: CalibrationMetadata
    rotation: list[list[list[float]]] | None = None

    @classmethod
    def from_file(cls: type[T], path: Path) -> T:
        with open(path) as fid:
            as_dict = yaml.safe_load(fid)
        model = cls(**as_dict)

        # If data is defined in a separate file load it, otherwise
        # leave it as is.
        if not model.metadata.data_filename:
            logger.debug(
                "Calibration layer data defined in metadata file. No separate file will be used."
            )
            return model

        data_file: Path = path.parent / model.metadata.data_filename.name

        if not data_file.exists():
            raise FileNotFoundError(f"Layer data file {data_file!s} not found.")

        logger.debug(f"Calibration layer data defined in separate file: {data_file!s}")
        return cls._load_data_file(data_file, model)

    @classmethod
    @abstractmethod
    def _load_data_file(cls: type[T], path: Path, existing_model: T) -> T: ...

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
            filepath.parent.mkdir(parents=True, exist_ok=True)

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
