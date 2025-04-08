import datetime
import logging
from collections.abc import Iterable
from functools import reduce
from pathlib import Path

import numpy as np
from spacepy import pycdf

from mag_toolkit.calibration.Calibrator import CalibrationMethod

from .CalibrationExceptions import CalibrationValidityError
from .calibrationFormat import (
    CalibrationLayer,
    CalibrationMetadata,
    CalibrationValue,
    ScienceLayer,
    ScienceValue,
    Validity,
)

logger = logging.getLogger(__name__)


class CalibrationApplicator:
    def apply(
        self,
        layer_files: list[Path],
        rotation: Path | None,
        dataFile,
        outputCalibrationFile: Path,
        outputScienceFile: Path,
    ) -> tuple[Path, Path]:
        """Currently operating on unprocessed data."""
        science_data = ScienceLayer.from_file(dataFile)

        if len(layer_files) < 1 and rotation is None:
            raise Exception("No layers to apply")

        if rotation is not None:
            science_data.values = self._rotate(rotation, science_data)

        # Could be memory intensive
        # TODO: Load and sum one set at a time to reduce mem usage (?)
        layers = [CalibrationLayer.from_file(layer_file) for layer_file in layer_files]
        sum_layer_values = reduce(self._sum_layers, [layer.values for layer in layers])

        validity = Validity(
            start=sum_layer_values[0].time, end=sum_layer_values[-1].time
        )

        # TODO: Correct dependenciues and science in cal and science file
        dependencies = [layer_file.name for layer_file in layer_files]
        metadata = CalibrationMetadata(
            dependencies=dependencies,
            science=[],
            creation_timestamp=datetime.datetime.now(),
        )
        calibrationLayer = CalibrationLayer(
            id="",
            mission=science_data.mission,
            validity=validity,
            method=CalibrationMethod.SUM,
            sensor=science_data.sensor,
            version=1,
            metadata=metadata,
            value_type="vector",
            values=sum_layer_values,  # type: ignore
        )

        cal_filepath = calibrationLayer.writeToFile(outputCalibrationFile)

        scienceResult = self._get_science_layer(
            science_data,
            dependencies,
            self._apply_layer_to_science_values(
                science_data.values, calibrationLayer.values
            ),
        )

        l2_filepath = scienceResult.writeToFile(outputScienceFile)

        return (l2_filepath, cal_filepath)

    def _rotate(self, rotation_filepath: Path, science_layer: ScienceLayer):
        with pycdf.CDF(str(rotation_filepath)) as cdf:
            rotation_matrices_mago = np.array(cdf["URFTOORFO"][...])
            rotation_matrices_magi = np.array(cdf["URFTOORFI"][...])  # noqa: F841
        for i, datapoint in enumerate(science_layer.values):
            appropriate_rotator = rotation_matrices_mago[datapoint.range]
            datapoint = np.matmul(appropriate_rotator, datapoint.value)
            science_layer.values[i].value = datapoint
        return science_layer.values

    def _get_science_layer(
        self, science: ScienceLayer, dependencies: list[str], values: list[ScienceValue]
    ):
        validity = Validity(start=values[0].time, end=values[-1].time)

        metadata = CalibrationMetadata(
            dependencies=dependencies,
            science=[science.science_file],
            creation_timestamp=datetime.datetime.now(),
        )
        return ScienceLayer(
            id="",
            mission=science.mission,
            validity=validity,
            sensor=science.sensor,
            version=0,
            metadata=metadata,
            science_file="",
            value_type="vector",
            values=values,
        )

    def _apply_layer_to_science_values(
        self,
        data_values: Iterable[ScienceValue],
        layer_values: Iterable[CalibrationValue],
    ):
        values = []

        for data_point, layer_point in zip(data_values, layer_values):
            if data_point.time != layer_point.time:
                raise Exception("Layer and data timestamps do not align")

            data_point_vector = np.array(data_point.value)
            layer_vector = np.array(layer_point.value)

            value = data_point_vector + layer_vector
            timedelta = layer_point.timedelta
            quality_flag = layer_point.quality_flag
            quality_bitmask = layer_point.quality_bitmask

            time = data_point.time + datetime.timedelta(seconds=timedelta)

            values.append(
                ScienceValue(
                    time=time,
                    value=list(value),
                    range=data_point.range,
                    quality_flag=quality_flag,
                    quality_bitmask=quality_bitmask,
                )
            )

        return values

    def _sum_layers(
        self,
        data_values: Iterable[CalibrationValue],
        layer_values: Iterable[CalibrationValue],
    ) -> list[CalibrationValue]:
        # Assume that the time stamps are exactly the same

        values = []

        for data_point, layer_point in zip(data_values, layer_values):
            if data_point.time != layer_point.time:
                raise Exception("Layer and data timestamps do not align")

            data_point_vector = np.array(data_point.value)
            layer_vector = np.array(layer_point.value)

            value = data_point_vector + layer_vector
            timedelta = data_point.timedelta + layer_point.timedelta

            quality_flag = data_point.quality_flag | layer_point.quality_flag
            quality_bitmask = data_point.quality_bitmask | layer_point.quality_bitmask

            values.append(
                CalibrationValue(
                    time=data_point.time,
                    value=list(value),
                    timedelta=timedelta,
                    quality_flag=quality_flag,
                    quality_bitmask=quality_bitmask,
                )
            )

        return values

    def checkValidity(self, data, calibrationCollection):
        # check for time validity
        if data.epoch[0] < np.datetime64(
            calibrationCollection.valid_start
        ) or data.epoch[1] > np.datetime64(calibrationCollection.valid_end):
            logging.debug("Data outside of calibration validity range")
            raise CalibrationValidityError("Data outside of calibration validity range")
