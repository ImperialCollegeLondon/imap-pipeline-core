import logging
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

import numpy as np

from mag_toolkit.calibration.Calibrator import CalibrationMethod

from .CalibrationExceptions import CalibrationValidityError
from .calibrationFormat import (
    CalibrationLayer,
    CalibrationMetadata,
    CalibrationValue,
    ScienceLayerZero,
    Validity,
    Value,
)

logger = logging.getLogger(__name__)


class CalibrationApplicator:
    def apply(self, layer_files: list[Path], dataFile, outputFile: Path) -> Path:
        """Currently operating on unprocessed data."""
        science_data = ScienceLayerZero.from_file(dataFile)

        if len(layer_files) < 1:
            raise Exception("No layers to apply")

        base_layer = science_data.values
        for layer_file in layer_files:
            layer = CalibrationLayer.from_file(layer_file)
            base_layer = self.apply_single(base_layer, layer.values)

        validity = Validity(start=base_layer[0].time, end=base_layer[-1].time)
        metadata = CalibrationMetadata(
            dependencies=[], science=[], creation_timestamp=datetime.now()
        )
        resultLayer = CalibrationLayer(
            id="result_id",
            mission=science_data.mission,
            validity=validity,
            method=CalibrationMethod.SUM,
            sensor=science_data.sensor,
            version=1,
            metadata=metadata,
            value_type="vector",
            values=base_layer,  # type: ignore
        )

        filepath = resultLayer.writeToFile(outputFile)

        return filepath

    def apply_single(
        self, data_values: Iterable[Value], layer_values: Iterable[Value]
    ) -> list[CalibrationValue]:
        # Assume that the time stamps are exactly the same

        values = []

        for data_point, layer_point in zip(data_values, layer_values):
            if data_point.time != layer_point.time:
                raise Exception("Layer and data timestamps do not align")
            data_point_vector = np.array(data_point.value)
            layer_vector = np.array(layer_point.value)
            value = data_point_vector + layer_vector

            values.append(
                CalibrationValue(time=data_point.time, value=list(value), timedelta=0)
            )

        return values

    def checkValidity(self, data, calibrationCollection):
        # check for time validity
        if data.epoch[0] < np.datetime64(
            calibrationCollection.valid_start
        ) or data.epoch[1] > np.datetime64(calibrationCollection.valid_end):
            logging.debug("Data outside of calibration validity range")
            raise CalibrationValidityError("Data outside of calibration validity range")
