import os
from pathlib import Path

import yaml
from pydantic import ValidationError

from .calibrationFormat import CalibrationFormat


class CalibrationFormatProcessor:
    def loadFromPath(calibrationPath: Path) -> CalibrationFormat:
        try:
            as_dict = yaml.safe_load(open(calibrationPath))
            model = CalibrationFormat(**as_dict)
            return model
        except ValidationError as e:
            print(e)
            return None
        except FileNotFoundError as e:
            print(e)
            return None

    def loadFromDict(calibrationDict: dict) -> CalibrationFormat:
        try:
            model = CalibrationFormat(**calibrationDict)
            return model
        except ValidationError as e:
            print(e)
            return None

    def getWriteable(CalibrationFormat: CalibrationFormat):
        json = CalibrationFormat.model_dump_json()

        return json

    def writeToFile(
        CalibrationFormat: CalibrationFormat, filepath: Path, createDirectory=False
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
