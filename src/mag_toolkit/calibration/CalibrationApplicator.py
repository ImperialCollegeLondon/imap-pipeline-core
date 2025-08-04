import logging
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from spacepy import pycdf

from mag_toolkit.calibration.CalibrationExceptions import CalibrationValidityError

from .CalibrationDefinitions import (
    CalibrationMetadata,
    CalibrationMethod,
    CalibrationValue,
    ScienceValue,
    Sensor,
    Validity,
    ValueType,
)
from .CalibrationLayer import CalibrationLayer
from .ScienceLayer import ScienceLayer

logger = logging.getLogger(__name__)


class CalibrationApplicator:
    def _apply_layers(
        self, layer_files: list[Path], science_values: list[ScienceValue]
    ) -> tuple[list[ScienceValue], list[CalibrationValue]]:
        total_offsets = []
        for layer in layer_files:
            if not layer.exists():
                raise FileNotFoundError(f"Layer file {layer} does not exist")
            calibration_layer = CalibrationLayer.from_file(layer)

            science_values, offsets = self._apply_layer_to_science_values(
                calibration_layer.value_type,
                science_values,
                calibration_layer.values,
            )
            if total_offsets:
                total_offsets = self._sum_layers(total_offsets, offsets)
            else:
                total_offsets = offsets

        return (science_values, total_offsets)

    def apply_rotation(
        self,
        rotation: Path,
        science_file: Path,
        outputL2File: Path,
    ) -> Path:
        """Apply rotation to the science data if a rotation file is provided."""
        science_data = ScienceLayer.from_file(science_file)
        science_values = self._rotate(rotation, science_data)
        scienceResult = self._construct_science_layer(
            science_data,
            [science_file.name, rotation.name],
            science_values,
        )

        l2_filepath = scienceResult.writeToFile(outputL2File)

        return l2_filepath

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
            raise ValueError("No calibration layers or rotation file provided")

        if rotation is not None:
            science_data.values = self._rotate(rotation, science_data)

        science_values = science_data.values

        dependencies = [layer_file.name for layer_file in layer_files]
        if rotation:
            dependencies.append(rotation.name)

        science_values, total_offsets = self._apply_layers(layer_files, science_values)

        offsets_layer = self._construct_calibration_layer(
            total_offsets,
            dependencies,
            science_data,
            outputCalibrationFile.name,
        )

        cal_filepath = offsets_layer.writeToFile(outputCalibrationFile)

        scienceResult = self._construct_science_layer(
            science_data,
            dependencies,
            science_values,
        )

        l2_filepath = scienceResult.writeToFile(outputScienceFile)

        return (l2_filepath, cal_filepath)

    def _construct_calibration_layer(
        self,
        offsets: list[CalibrationValue],
        dependencies: list[str],
        original_science: ScienceLayer,
        calibration_id: str,
    ) -> CalibrationLayer:
        """Construct a calibration layer from the provided layer files."""

        validity = Validity(start=offsets[0].time, end=offsets[-1].time)

        # TODO: Correct dependencies and science in cal and science file

        offsets_metadata = CalibrationMetadata(
            dependencies=dependencies,
            science=[original_science.science_file],
            creation_timestamp=np.datetime64("now"),
        )
        offsets_layer = CalibrationLayer(
            id=calibration_id,
            mission=original_science.mission,
            validity=validity,
            method=CalibrationMethod.SUM,
            sensor=original_science.sensor,
            version=1,
            metadata=offsets_metadata,
            value_type=ValueType.VECTOR,
            values=offsets,  # type: ignore
        )

        return offsets_layer

    def _rotate(self, rotation_filepath: Path, science_layer: ScienceLayer):
        with pycdf.CDF(str(rotation_filepath)) as cdf:
            rotation_matrices_mago = np.array(cdf["URFTOORFO"][...])
            rotation_matrices_magi = np.array(cdf["URFTOORFI"][...])
        rotation_matrix = (
            rotation_matrices_mago
            if science_layer.sensor == Sensor.MAGO
            else rotation_matrices_magi
        )
        for i, datapoint in enumerate(science_layer.values):
            appropriate_rotator = rotation_matrix[datapoint.range]
            datapoint = np.matmul(appropriate_rotator, datapoint.value)
            science_layer.values[i].value = datapoint
        return science_layer.values

    def _construct_science_layer(
        self, science: ScienceLayer, dependencies: list[str], values: list[ScienceValue]
    ):
        validity = Validity(start=values[0].time, end=values[-1].time)

        metadata = CalibrationMetadata(
            dependencies=dependencies,
            science=[science.science_file],
            creation_timestamp=np.datetime64("now"),
        )
        science_layer = ScienceLayer(
            id="",
            mission=science.mission,
            validity=validity,
            sensor=science.sensor,
            version=0,
            metadata=metadata,
            science_file="",
            value_type=ValueType.VECTOR,
            values=values,
        )
        science_layer = science_layer.calculate_magnitudes()
        return science_layer

    def _apply_layer_to_science_values(
        self,
        layer_value_type: ValueType,
        data_values: Iterable[ScienceValue],
        layer_values: Iterable[CalibrationValue],
    ) -> tuple[list[ScienceValue], list[CalibrationValue]]:
        match layer_value_type:
            case ValueType.VECTOR:
                return self._apply_vector_layer_to_science_values(
                    data_values, layer_values
                )
            case ValueType.INTERPOLATION_POINTS:
                return self._apply_interpolation_points_to_science_values(
                    data_values, layer_values
                )
            case _:
                raise ValueError(f"Unsupported layer value type: {layer_value_type}")

    def _apply_interpolation_points_to_science_values(
        self,
        data_values: Iterable[ScienceValue],
        layer_values: Iterable[CalibrationValue],
    ) -> tuple[list[ScienceValue], list[CalibrationValue]]:
        science_times = [data_point.time.timestamp() for data_point in data_values]
        x_vals = np.interp(
            science_times,
            [layer_point.time.timestamp() for layer_point in layer_values],
            [layer_point.value[0] for layer_point in layer_values],
        )
        y_vals = np.interp(
            science_times,
            [layer_point.time.timestamp() for layer_point in layer_values],
            [layer_point.value[1] for layer_point in layer_values],
        )
        z_vals = np.interp(
            science_times,
            [layer_point.time.timestamp() for layer_point in layer_values],
            [layer_point.value[2] for layer_point in layer_values],
        )
        interpolated_quality_flags = np.interp(
            science_times,
            [layer_point.time.timestamp() for layer_point in layer_values],
            [layer_point.quality_flag for layer_point in layer_values],
        )
        interpolated_quality_bitmasks = np.interp(
            science_times,
            [layer_point.time.timestamp() for layer_point in layer_values],
            [layer_point.quality_bitmask for layer_point in layer_values],
        )
        interpolated_calibration_values = [
            CalibrationValue(
                time=datetime.fromtimestamp(science_time),
                value=[x_val, y_val, z_val],
                timedelta=0,
                quality_flag=int(interpolated_quality_flag),
                quality_bitmask=int(interpolated_quality_bitmask),
            )
            for science_time, x_val, y_val, z_val, interpolated_quality_flag, interpolated_quality_bitmask in zip(
                science_times,
                x_vals,
                y_vals,
                z_vals,
                interpolated_quality_flags,
                interpolated_quality_bitmasks,
            )
        ]

        return self._apply_vector_layer_to_science_values(
            data_values, interpolated_calibration_values
        )

    def _apply_vector_layer_to_science_values(
        self,
        data_values: Iterable[ScienceValue],
        layer_values: Iterable[CalibrationValue],
    ) -> tuple[list[ScienceValue], list[CalibrationValue]]:
        # This method assumes that the layer values are vectors and applies them to the science values
        values: list[ScienceValue] = []

        for data_point, layer_point in zip(data_values, layer_values):
            if data_point.time != layer_point.time:
                raise ValueError("Layer and data timestamps do not align")

            data_point_vector = np.array(data_point.value)
            layer_vector = np.array(layer_point.value)

            value = data_point_vector + layer_vector
            timedelta_value = layer_point.timedelta
            quality_flag = layer_point.quality_flag
            quality_bitmask = layer_point.quality_bitmask

            time = data_point.time + pd.to_timedelta(timedelta_value, "s").to_numpy()

            values.append(
                ScienceValue(
                    time=time,
                    value=list(value),
                    range=data_point.range,
                    quality_flag=quality_flag,
                    quality_bitmask=quality_bitmask,
                )
            )

        return (values, list(layer_values))

    def _sum_layers(
        self,
        data_values: Iterable[CalibrationValue],
        layer_values: Iterable[CalibrationValue],
    ) -> list[CalibrationValue]:
        values = []

        for data_point, layer_point in zip(data_values, layer_values):
            if data_point.time != layer_point.time:
                raise ValueError("Layer and data timestamps do not align")

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

    def check_science_in_valid_calibration_window(
        self, data: ScienceLayer, calibration_layer: CalibrationLayer
    ):
        # check for time validity
        if data.values[0].time < np.datetime64(
            calibration_layer.validity.start
        ) or data.values[-1].time > np.datetime64(calibration_layer.validity.end):
            logger.debug("Data outside of calibration validity range")
            raise CalibrationValidityError("Data outside of calibration validity range")
