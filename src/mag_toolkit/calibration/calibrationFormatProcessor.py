import os
from pathlib import Path

import yaml
from pydantic import ValidationError

from .calibrationFormat import CalibrationLayer


class CalibrationFormatProcessor:
    @staticmethod
    def loadFromPath(calibrationPath: Path) -> CalibrationLayer | None:
        try:
            as_dict = yaml.safe_load(open(calibrationPath))
            model = CalibrationLayer(**as_dict)
            return model
        except ValidationError as e:
            print(e)
            return None
        except FileNotFoundError as e:
            print(e)
            return None

    @staticmethod
    def loadFromDict(calibrationDict: dict) -> CalibrationLayer | None:
        try:
            model = CalibrationLayer(**calibrationDict)
            return model
        except ValidationError as e:
            print(e)
            return None

    @staticmethod
    def getWriteable(CalibrationLayer: CalibrationLayer):
        json = CalibrationLayer.model_dump_json()

        return json

    @staticmethod
    def writeToFile(
        CalibrationFormat: CalibrationLayer, filepath: Path, createDirectory=False
    ):
        json = CalibrationFormat.model_dump_json()

        if createDirectory:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

        try:
            with open(filepath, "w+") as f:
                f.write(json)
        except Exception as e:
            print(e)
            print(f"Failed to write calibration to {filepath}")

        return filepath
