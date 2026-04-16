from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas
import spiceypy
from cdflib.xarray import cdf_to_xarray, xarray_to_cdf
from imap_processing.mag.l2 import mag_l2, mag_l2_data
from imap_processing.mag.l2.mag_l2_data import ValidFrames

from imap_mag.cli.fetch.spice import generate_spice_metakernel
from imap_mag.config import AppSettings
from imap_mag.io.file import SciencePathHandler
from imap_mag.io.FilePathHandlerSelector import AncillaryPathHandler
from imap_mag.util import ScienceMode
from imap_mag.util.ReferenceFrame import ReferenceFrame
from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
)
from mag_toolkit.calibration.CalibrationExceptions import CalibrationValidityError

from .CalibrationDefinitions import (
    CalibrationMethod,
    ValueType,
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

        science.load_contents()
        science_epochs = science._contents[CONSTANTS.CSV_VARS.EPOCH]

        offsets = CalibrationLayer.from_file(layers[0], load_contents=True)
        if offsets.value_type == ValueType.BOUNDARY_CHANGES_ONLY:
            offsets = self._expand_boundary_changes_to_every_epoch(
                offsets, science_epochs
            )

        if not offsets.compatible(science):
            raise CalibrationValidityError(
                "Offsets and science data are not time compatible"
            )
            # NOTE: later layers are checked to be compatible with the previous when summing them
        science.clear_contents()

        offsets = self._init_base_layer(offsets)

        for layer_file in layers[1:]:
            layer = CalibrationLayer.from_file(layer_file, load_contents=True)
            if layer.value_type == ValueType.BOUNDARY_CHANGES_ONLY:
                layer = self._expand_boundary_changes_to_every_epoch(
                    layer, science_epochs
                )
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
        spice_metakernel: Path | None = None,
        reference_frames: list[ReferenceFrame] = [
            ReferenceFrame.SRF,
            ReferenceFrame.GSE,
            ReferenceFrame.GSM,
            ReferenceFrame.RTN,
            ReferenceFrame.DSRF,
        ],
    ) -> tuple[list[Path], Path]:
        """Currently operating on unprocessed data."""

        if len(layer_files) < 1 and rotation is None:
            raise ValueError("No calibration layers or rotation file provided")

        if not outputScienceFolder.exists():
            raise ValueError(
                f"Output science folder does not exist: {outputScienceFolder}"
            )

        science_handler = SciencePathHandler.from_filename(dataFile.name)
        if not dataFile.exists() or not science_handler:
            raise ValueError(
                f"Input science file does not exist or could not be parsed: {dataFile}"
            )

        if outputOffsetsFile.exists():
            logger.warning(
                f"Output calibration file already exists and would be overwritten: {outputOffsetsFile}"
            )
            raise FileExistsError(
                f"Output calibration file already exists and would be overwritten: {outputOffsetsFile}"
            )

        science = ScienceLayer.from_file(dataFile, load_contents=True)

        created_offsets_filepath = self._create_offsets_file(
            layer_files,
            outputOffsetsFile,
            science=science,
        )

        del science

        if not reference_frames:
            logger.info(
                "No reference frames specified, only offsets have been created, skipping L2 file generation"
            )
            return [], created_offsets_filepath

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
        if spice_metakernel is None:
            spice_metakernel = generate_spice_metakernel(
                start_time=day_to_process + timedelta(hours=-1),
                end_time=day_to_process
                + timedelta(
                    days=1, hours=1
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
            )  # type: ignore

        if spice_metakernel is None:
            raise ValueError("Failed to generate spice metakernel for L2 generation")

        resolved_mk_path = spice_metakernel.resolve()

        if not resolved_mk_path.exists():
            raise ValueError(
                f"Resolved spice metakernel path does not exist: {resolved_mk_path}"
            )

        logger.info(f"furnishing spice metakernel at {resolved_mk_path}")

        original_cwd = Path.cwd()
        try:
            os.chdir(self.app_settings.data_store)
            spiceypy.kclear()
            spiceypy.furnsh(str(resolved_mk_path))
            os.chdir(original_cwd)
            logger.info("Kernels furnished. Loading data ready for L2 file generation")
            science_data = cdf_to_xarray(str(dataFile), to_datetime=False)
            created_offsets_data = cdf_to_xarray(
                str(created_offsets_filepath), to_datetime=False
            )
            mode = (
                mag_l2_data.DataMode.NORM
                if science_handler.get_mode() == ScienceMode.Normal
                else mag_l2_data.DataMode.BURST
            )
            logger.info(
                f"Using calibration to create L2 file(s) for {dataFile.name} in {mode.value} mode in frames {reference_frames} using imap_processing.mag.l2.mag_l2"
            )
            datasets = mag_l2.mag_l2(
                input_data=science_data,
                calibration_dataset=calibration_dataset,
                offsets_dataset=created_offsets_data,
                mode=mode,
                day_to_process=np.datetime64(day_to_process),
                frames=CalibrationApplicator._get_l2_frames(reference_frames),
            )

        finally:
            os.chdir(original_cwd)
            spiceypy.kclear()
            del science_data
            del created_offsets_data

        # log paths to all files
        logger.info(
            f"Applied calibration layer(s) and created {len(datasets)} output dataset(s). Filtering to frames {reference_frames} and writing to CDF files in {outputScienceFolder}"
        )

        allowed_frame_descriptor_segments = [
            f"-{frame.value}" for frame in reference_frames
        ]

        files_created = []
        for ds in datasets:
            logical_source = str(ds.attrs.get("Logical_source"))
            if not logical_source:
                logger.warning(
                    "No Logical_source attribute found in dataset, skipping file naming"
                )
                continue
            if not any(
                segment in logical_source
                for segment in allowed_frame_descriptor_segments
            ):
                logger.info(
                    f"Skipping dataset with Logical_source {logical_source} as it does not match allowed frames {reference_frames}"
                )
                continue

            if "_l2_" in logical_source and "l2-pre" not in logical_source:
                logical_source = logical_source.replace(
                    "l2", "l2-pre"
                )  # set level to l2-pre for the output file naming, as it's pre-release l2

            filename = SciencePathHandler.generate_filename_from_logical_source(
                logical_source=logical_source,
                content_date=day_to_process,
                version=1,
                extension="cdf",
            )
            filepath = outputScienceFolder / filename

            ds.attrs["Logical_file_id"] = filename
            logger.info(f"Writing output science file to {filepath}")
            if filepath.exists():
                logger.warning(
                    f"Output science file already exists and would be overwritten: {filepath}"
                )
                raise FileExistsError(
                    f"Output science file already exists and would be overwritten: {filepath}"
                )
            xarray_to_cdf(ds, str(filepath))
            files_created.append(filepath)

        return (files_created, created_offsets_filepath)

    @staticmethod
    def _get_l2_frames(reference_frames: list[ReferenceFrame]) -> list[ValidFrames]:
        frame_mapping = {
            ReferenceFrame.DSRF: ValidFrames.DSRF,
            ReferenceFrame.SRF: ValidFrames.SRF,
            ReferenceFrame.GSE: ValidFrames.GSE,
            ReferenceFrame.GSM: ValidFrames.GSM,
            ReferenceFrame.RTN: ValidFrames.RTN,
        }
        return [
            frame_mapping[frame] for frame in reference_frames if frame in frame_mapping
        ]

    def _expand_boundary_changes_to_every_epoch(
        self,
        layer: CalibrationLayer,
        science_epochs: pandas.Series,
    ) -> CalibrationLayer:
        """Expand an BOUNDARY_CHANGES_ONLY layer to match science timestamps using forward-fill."""
        import pandas as pd

        layer.load_contents()
        if layer._contents is None:
            raise ValueError("Boundary changes layer has no contents to expand.")

        change_points = layer._contents.copy()
        change_points[CONSTANTS.CSV_VARS.EPOCH] = pd.to_datetime(
            change_points[CONSTANTS.CSV_VARS.EPOCH]
        )
        change_points = change_points.set_index(CONSTANTS.CSV_VARS.EPOCH)

        science_index = pd.DatetimeIndex(science_epochs)

        if change_points.empty:
            zero_df = pd.DataFrame(
                {
                    CONSTANTS.CSV_VARS.OFFSET_X: 0.0,
                    CONSTANTS.CSV_VARS.OFFSET_Y: 0.0,
                    CONSTANTS.CSV_VARS.OFFSET_Z: 0.0,
                    CONSTANTS.CSV_VARS.TIMEDELTA: 0.0,
                    CONSTANTS.CSV_VARS.QUALITY_FLAG: pandas.array(
                        [0] * len(science_index), dtype=pandas.Int64Dtype()
                    ),
                    CONSTANTS.CSV_VARS.QUALITY_BITMASK: pandas.array(
                        [0] * len(science_index), dtype=pandas.Int64Dtype()
                    ),
                },
                index=science_index,
            )
            zero_df.index.name = CONSTANTS.CSV_VARS.EPOCH
            zero_df = zero_df.reset_index()
            layer._contents = zero_df
            layer.value_type = ValueType.VECTOR
            return layer

        first_science_epoch = science_index[0]
        first_change_time = change_points.index[0]

        if first_change_time > first_science_epoch:
            zero_row = pd.DataFrame(
                {
                    CONSTANTS.CSV_VARS.OFFSET_X: [0.0],
                    CONSTANTS.CSV_VARS.OFFSET_Y: [0.0],
                    CONSTANTS.CSV_VARS.OFFSET_Z: [0.0],
                    CONSTANTS.CSV_VARS.TIMEDELTA: [0.0],
                    CONSTANTS.CSV_VARS.QUALITY_FLAG: pandas.array(
                        [0], dtype=pandas.Int64Dtype()
                    ),
                    CONSTANTS.CSV_VARS.QUALITY_BITMASK: pandas.array(
                        [0], dtype=pandas.Int64Dtype()
                    ),
                },
                index=pd.DatetimeIndex([first_science_epoch]),
            )
            change_points = pd.concat([zero_row, change_points])

        expanded = change_points.reindex(science_index, method="ffill")

        expanded[CONSTANTS.CSV_VARS.TIMEDELTA] = expanded[
            CONSTANTS.CSV_VARS.TIMEDELTA
        ].fillna(0.0)

        # Defensive check: quality_flag and quality_bitmask must never be NaN after expansion
        for col in [
            CONSTANTS.CSV_VARS.QUALITY_FLAG,
            CONSTANTS.CSV_VARS.QUALITY_BITMASK,
        ]:
            if expanded[col].isna().any():
                raise ValueError(
                    f"Unexpected NaN values in '{col}' after layer expansion. "
                    f"Layer files must not contain NaN quality values."
                )

        expanded = expanded.reset_index()
        expanded = expanded.rename(columns={"index": CONSTANTS.CSV_VARS.EPOCH})

        layer._contents = expanded
        layer.value_type = ValueType.VECTOR
        return layer

    def _init_base_layer(self, offsets: CalibrationLayer) -> CalibrationLayer:
        if offsets._contents is None:
            raise ValueError("Offset contents are not loaded")

        # In the base layer quality_flag starts at 0 for all epochs.
        # -1 (clear) applied to an all-zero base still gives 0.
        flag_col = offsets._contents[CONSTANTS.CSV_VARS.QUALITY_FLAG].astype(
            pandas.Int64Dtype()
        )
        offsets._contents[CONSTANTS.CSV_VARS.QUALITY_FLAG] = flag_col.where(
            flag_col != -1, 0
        )

        # In the base layer bitmask starts at zero.
        # Negative values (bit-clear) applied to 0 also give 0.
        bitmask_col = offsets._contents[CONSTANTS.CSV_VARS.QUALITY_BITMASK].astype(
            pandas.Int64Dtype()
        )
        offsets._contents[CONSTANTS.CSV_VARS.QUALITY_BITMASK] = bitmask_col.where(
            bitmask_col >= 0, 0
        )

        offsets._data_path = None  # Invalidate data path since contents have changed
        return offsets

    def _sum_layers(
        self,
        offsets: CalibrationLayer,
        layer: CalibrationLayer,
    ) -> CalibrationLayer:
        if offsets._contents is None or layer._contents is None:
            raise ValueError("Offsets or layer contents are not loaded")

        if not offsets.compatible(layer):
            raise ValueError("Offsets and layer are not time compatible")

        cols = CONSTANTS.CSV_VARS

        # If the base offset is NaN it stays NaN, otherwise add the layer offset (NaN propagates in float arithmetic)
        offsets._contents[cols.OFFSET_X] = (
            offsets._contents[cols.OFFSET_X] + layer._contents[cols.OFFSET_X]
        )
        offsets._contents[cols.OFFSET_Y] = (
            offsets._contents[cols.OFFSET_Y] + layer._contents[cols.OFFSET_Y]
        )
        offsets._contents[cols.OFFSET_Z] = (
            offsets._contents[cols.OFFSET_Z] + layer._contents[cols.OFFSET_Z]
        )
        offsets._contents[cols.TIMEDELTA] = (
            offsets._contents[cols.TIMEDELTA] + layer._contents[cols.TIMEDELTA]
        )

        # quality_flag combining via bitwise OR semantics (no NaN values permitted):
        #   0  → no change (OR with 0 leaves existing value)
        #   1  → set the flag (OR with 1 always gives 1)
        #  -1  → clear the flag to 0
        layer_flag_column = layer._contents[cols.QUALITY_FLAG].astype(
            pandas.Int64Dtype()
        )
        base_flag_column = offsets._contents[cols.QUALITY_FLAG].astype(
            pandas.Int64Dtype()
        )
        clear_flag_mask = layer_flag_column == -1
        # Step 1: where layer is -1, force base to 0; otherwise keep base as-is
        base_after_clear = base_flag_column.where(~clear_flag_mask, 0)
        # Step 2: OR the positive part of the layer (0 or 1 only)
        positive_layer_flag = layer_flag_column.clip(lower=0)
        offsets._contents[cols.QUALITY_FLAG] = base_after_clear | positive_layer_flag

        # quality_bitmask combining (no NaN values permitted):
        #   0          → no change
        #   positive N → OR N into mask (sets those bits)
        #   negative N → clear those bits: result = base & (N - 1)  [since ~(-N) == N - 1]
        layer_bitmask_column = layer._contents[cols.QUALITY_BITMASK].astype(
            pandas.Int64Dtype()
        )
        offsets_bitmask_column = offsets._contents[cols.QUALITY_BITMASK].astype(
            pandas.Int64Dtype()
        )

        or_mask = layer_bitmask_column > 0
        clear_mask = layer_bitmask_column < 0

        result_bitmask = offsets_bitmask_column.copy()
        # OR positive values into the mask
        result_bitmask = result_bitmask.where(
            ~or_mask, offsets_bitmask_column | layer_bitmask_column
        )
        # Negative N clears bits: result = base & (layer - 1)  [~(-layer) == layer - 1]
        result_bitmask = result_bitmask.where(
            ~clear_mask, offsets_bitmask_column & (layer_bitmask_column - 1)
        )
        offsets._contents[cols.QUALITY_BITMASK] = result_bitmask

        offsets._data_path = None  # Invalidate data path since contents have changed
        return offsets
