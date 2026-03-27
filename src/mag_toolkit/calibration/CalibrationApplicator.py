import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import spiceypy
from cdflib.xarray import cdf_to_xarray, xarray_to_cdf
from imap_processing.mag.l2 import mag_l2, mag_l2_data

from imap_mag.cli.fetch.spice import generate_spice_metakernel
from imap_mag.config import AppSettings
from imap_mag.io.file import SciencePathHandler
from imap_mag.io.FilePathHandlerSelector import AncillaryPathHandler
from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
)
from mag_toolkit.calibration.CalibrationExceptions import CalibrationValidityError

from .CalibrationDefinitions import (
    CalibrationMethod,
)
from .CalibrationLayer import CalibrationLayer
from .CalibrationMatrix import CalibrationMatrix
from .ScienceLayer import ScienceLayer

logger = logging.getLogger(__name__)


class CalibrationApplicator:
    def __init__(self, app_settings: AppSettings = AppSettings()):
        self.app_settings = app_settings

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

        offsets.set_metadata(
            [science.science_file],
            science,
            outputCalibrationFile.name,
            method=CalibrationMethod.SUM,
        )

        cal_filepath = offsets.writeToFile(outputCalibrationFile)

        return cal_filepath

    def apply(
        self,
        day_to_process: datetime,
        layer_files: list[Path],
        rotation: Path | None,
        dataFile: Path,  # the file path of the science file that gived the raw data the layers are applied to
        outputOffsetsFile: Path,
        outputScienceFolder: Path,
    ) -> tuple[list[Path], Path]:
        """Currently operating on unprocessed data."""

        if len(layer_files) < 1 and rotation is None:
            raise ValueError("No calibration layers or rotation file provided")

        if not outputScienceFolder.exists():
            raise ValueError(
                f"Output science folder does not exist: {outputScienceFolder}"
            )

        if not dataFile.exists():
            raise ValueError(f"Input science file does not exist: {dataFile}")

        if outputOffsetsFile.exists():
            logger.warning(
                f"Output calibration file already exists and will be overwritten: {outputOffsetsFile}"
            )

        science = ScienceLayer.from_file(dataFile, load_contents=True)

        cal_filepath = self._create_offsets_file(
            layer_files,
            outputOffsetsFile,
            science=science,
        )

        del science

        if rotation is None:
            rotationCalibrationDataset = CalibrationMatrix.get_zero_rotation_dataset()
            version = 0
        else:
            handler = AncillaryPathHandler.from_filename(rotation)
            if not handler:
                raise ValueError(f"Could not parse rotation file name: {rotation}")
            version = handler.version
            rotationCalibrationDataset = (
                CalibrationMatrix.get_rotation_dataset_by_cdf_file(rotation)
            )

        calibration_dataset = (
            CalibrationMatrix.get_combined_epoch_dataset_for_imap_processing(
                rotationCalibrationDataset, day_to_process, day_to_process, version
            )
        )

        # need to get the spice furnished as the l2 step does time truncation needed clock kernels and rotations
        path_to_mk = generate_spice_metakernel(
            start_time=day_to_process + timedelta(hours=-12),
            end_time=day_to_process
            + timedelta(
                days=1, hours=12
            ),  # ensure we have plenty of spice coverage around it
            file_types=[
                "leapseconds",
                "planetary_constants",
                "science_frames",
                "imap_frames",
                "spacecraft_clock",
                "attitude_history",
                "pointing_attitude",
                "planetary_ephemeris",
                "ephemeris_reconstructed",
            ],
            verify=False,
            # base_path=Path(".") # TODO: work out if we need to path the MK
        )
        resolved_mk_path: str = str(path_to_mk.resolve())  # type: ignore

        logger.info(f"furnishing spice metakernel at {resolved_mk_path}")

        original_cwd = Path.cwd()
        try:
            os.chdir(self.app_settings.data_store)
            spiceypy.kclear()
            spiceypy.furnsh(resolved_mk_path)
            datasets = mag_l2.mag_l2(
                input_data=cdf_to_xarray(str(dataFile), to_datetime=False),
                calibration_dataset=calibration_dataset,
                offsets_dataset=cdf_to_xarray(str(cal_filepath), to_datetime=False),
                mode=mag_l2_data.DataMode.NORM,
                day_to_process=np.datetime64(day_to_process),
            )

        finally:
            os.chdir(original_cwd)
            spiceypy.kclear()
            os.remove(resolved_mk_path)  # clean up the generated metakernel

        # these are the datasets in order created by mag_l2,
        # for frame in [
        #     ValidFrames.SRF,
        #     ValidFrames.GSE,
        #     ValidFrames.GSM,
        #     ValidFrames.RTN,
        #     ValidFrames.DSRF,
        # ]:

        # log paths to all files
        logger.info(
            f"Applied calibration layer(s) and create {len(datasets)} output dataset(s)"
        )

        files_created = []
        for ds in datasets:
            logical_source = str(ds.attrs.get("Logical_source"))
            if not logical_source:
                logger.warning(
                    "No Logical_source attribute found in dataset, skipping file naming"
                )
                continue
            if "_l2_" in logical_source and "l2-pre" not in logical_source:
                logical_source = logical_source.replace(
                    "l2", "l2-pre"
                )  # set level to l2-pre for the output file naming, as it's pre-release l2

            filename = SciencePathHandler.generate_filename_from_logical_source(
                logical_source=logical_source,
                content_date=day_to_process,
                version=0,
                extension="cdf",
            )
            filepath = outputScienceFolder / filename

            ds.attrs["Logical_file_id"] = filename
            logger.info(f"Writing output science file to {filepath}")
            xarray_to_cdf(ds, str(filepath))
            files_created.append(filepath)

        return (files_created, cal_filepath)

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
