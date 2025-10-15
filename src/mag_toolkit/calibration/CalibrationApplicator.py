import logging
from datetime import datetime
from pathlib import Path

import numpy as np
from cdflib.xarray import cdf_to_xarray, xarray_to_cdf
from imap_processing.mag.l2 import mag_l2, mag_l2_data

from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
)
from mag_toolkit.calibration.CalibrationExceptions import CalibrationValidityError

from .CalibrationDefinitions import (
    CalibrationMetadata,
    CalibrationMethod,
    Validity,
    ValueType,
)
from .CalibrationLayer import CalibrationLayer
from .ScienceLayer import ScienceLayer

logger = logging.getLogger(__name__)


class CalibrationApplicator:
    def _create_offsets_file(
        self,
        layers: list[Path],
        outputCalibrationFile: Path,
        science: ScienceLayer,
    ) -> Path:
        if len(layers) < 1:
            raise ValueError("No calibration layers provided")

        offsets = CalibrationLayer.from_file(layers[0], load_contents=True)

        if not offsets.compatible(science):
            raise CalibrationValidityError(
                "Offsets and science data are not time compatible"
            )
        science.clear_contents()

        for layer_file in layers[1:]:
            layer = CalibrationLayer.from_file(layer_file, load_contents=True)
            offsets = self._sum_layers(offsets, layer)
            del layer

        if offsets._contents is None:
            raise ValueError("Offsets layer contents not loaded")

        offsets_layer = self._set_metadata(
            offsets,
            [science.science_file],
            science,
            outputCalibrationFile.name,
        )

        cal_filepath = offsets_layer.writeToFile(outputCalibrationFile)

        return cal_filepath

    def _set_metadata(
        self,
        offsets: CalibrationLayer,
        dependencies: list[str],
        original_science: ScienceLayer,
        calibration_id: str,
    ) -> CalibrationLayer:
        """Set the metadata for the offsets layer based on the original science layer."""
        if offsets._contents is None:
            raise ValueError("Offsets layer contents not loaded")

        validity = Validity(
            start=offsets._contents[0].time, end=offsets._contents[-1].time
        )

        offsets_metadata = CalibrationMetadata(
            dependencies=dependencies,
            science=[original_science.science_file],
            creation_timestamp=np.datetime64("now"),
        )
        offsets.metadata = offsets_metadata
        offsets.validity = validity
        offsets.id = calibration_id
        offsets.version = 1
        offsets.method = CalibrationMethod.SUM
        offsets.value_type = ValueType.VECTOR
        offsets.sensor = original_science.sensor
        offsets.mission = original_science.mission

        return offsets

    def apply(
        self,
        day_to_process: datetime,
        layer_files: list[Path],
        rotation: Path | None,
        dataFile,
        outputCalibrationFile: Path,
        outputScienceFile: Path,
    ) -> tuple[Path, Path]:
        """Currently operating on unprocessed data."""

        if len(layer_files) < 1 and rotation is None:
            raise ValueError("No calibration layers or rotation file provided")

        science = ScienceLayer.from_file(dataFile, load_contents=True)

        cal_filepath = self._create_offsets_file(
            layer_files,
            outputCalibrationFile,
            science=science,
        )

        del science

        datasets = mag_l2.mag_l2(
            input_data=cdf_to_xarray(str(dataFile), to_datetime=False),
            calibration_dataset=cdf_to_xarray(str(rotation), to_datetime=False),
            offsets_dataset=cdf_to_xarray(str(cal_filepath), to_datetime=False),
            mode=mag_l2_data.DataMode.NORM,
            day_to_process=np.datetime64(day_to_process),
        )

        for ds in datasets:
            ds.attrs["Logical_file_id"] = outputScienceFile.name
            xarray_to_cdf(ds, str(outputScienceFile))
            break  # only write one file for now

        return (outputScienceFile, cal_filepath)

    def _sum_layers(
        self,
        offsets: CalibrationLayer,
        layer: CalibrationLayer,
    ) -> CalibrationLayer:
        if offsets._contents is None or layer._contents is None:
            raise ValueError("Offsets or layer contents are not loaded")

        if not offsets.compatible(layer):
            raise ValueError("Offsets and layer are not time compatible")

        offsets._contents[CONSTANTS.CSV_VARS.OFFSET_X] += layer._contents[
            CONSTANTS.CSV_VARS.OFFSET_X
        ]
        offsets._contents[CONSTANTS.CSV_VARS.OFFSET_Y] += layer._contents[
            CONSTANTS.CSV_VARS.OFFSET_Y
        ]
        offsets._contents[CONSTANTS.CSV_VARS.OFFSET_Z] += layer._contents[
            CONSTANTS.CSV_VARS.OFFSET_Z
        ]
        offsets._contents[CONSTANTS.CSV_VARS.TIMEDELTA] += layer._contents[
            CONSTANTS.CSV_VARS.TIMEDELTA
        ]
        offsets._contents[CONSTANTS.CSV_VARS.QUALITY_FLAG] |= layer._contents[
            CONSTANTS.CSV_VARS.QUALITY_FLAG
        ]
        offsets._contents[CONSTANTS.CSV_VARS.QUALITY_BITMASK] |= layer._contents[
            CONSTANTS.CSV_VARS.QUALITY_BITMASK
        ]

        offsets._data_path = None  # Invalidate data path since contents have changed
        return offsets
