import logging
import typing
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd
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
    _data_path: Path | None = None
    _contents: pd.DataFrame | None = None

    @classmethod
    def from_file(cls: type[T], path: Path, load_contents=True) -> T:
        logger.info(f"Loading calibration layer from {path!s}.")
        with open(path) as fid:
            as_dict = yaml.safe_load(fid)
        model = cls(**as_dict)

        if model.metadata.data_filename is None:
            raise ValueError("Layer file does not specify a data filename.")

        data_file: Path = path.parent / model.metadata.data_filename.name

        model._data_path = data_file

        if not data_file.exists():
            raise FileNotFoundError(f"Layer data file {data_file!s} not found.")

        logger.debug(f"Calibration layer data defined in separate file: {data_file!s}")
        if load_contents:
            model._load_data_file(data_file)
        return model

    def clear_contents(self: T) -> T:
        self._contents = None
        return self

    def load_contents(self: T) -> T:
        if self._contents is not None:
            logger.debug("Layer contents already loaded.")
            return self
        if self._data_path is None:
            raise ValueError("Layer has no associated path for data.")

        return self._load_data_file(self._data_path)

    @abstractmethod
    def _load_data_file(self: T, path: Path) -> T: ...

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
