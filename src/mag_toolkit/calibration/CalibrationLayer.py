import logging
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import cdflib as lib
import numpy as np
import pandas as pd
import xarray as xr
from cdflib.xarray import cdf_to_xarray, xarray_to_cdf
from pydantic import PrivateAttr

from imap_mag.io.file import CalibrationLayerPathHandler
from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
    CalibrationMetadata,
    CalibrationMethod,
    Mission,
    Sensor,
    ValueType,
)
from mag_toolkit.calibration.Layer import Layer, Validity
from mag_toolkit.calibration.ScienceLayer import ScienceLayer

logger = logging.getLogger(__name__)


class CalibrationLayer(Layer):
    method: CalibrationMethod
    value_type: ValueType
    _contents: pd.DataFrame | None = PrivateAttr(default=None)

    def _write_to_csv(self, filepath: Path, createDirectory=False):
        if self._contents is None:
            raise ValueError("No contents loaded to write to CSV.")
        if createDirectory:
            filepath.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Writing calibration layer CSV to {filepath!s}.")
        self._contents.to_csv(
            filepath,
            index=False,
            header=True,
            date_format="%Y-%m-%dT%H:%M:%S.%f",
        )
        return filepath

    def get_epochs(self) -> pd.Series:
        """Get the epochs from the calibration layer contents."""
        self.load_contents()
        if self._contents is None:
            raise ValueError("No contents loaded to get epochs from.")
        return self._contents[CONSTANTS.CSV_VARS.EPOCH]

    def compatible(self, other: Layer) -> bool:
        """Check if another calibration layer is time compatible with this one."""
        self.load_contents()
        other.load_contents()

        if self._contents is None or other._contents is None:
            raise ValueError("One of the layers has no data.")

        # print the data types of both epoch columns for debugging
        logger.debug(
            f"Self epochs dtype: {self._contents[CONSTANTS.CSV_VARS.EPOCH].dtype}, "
            f"Other epochs dtype: {other._contents[CONSTANTS.CSV_VARS.EPOCH].dtype}"
        )

        # compare the lenth of the epoch columns first for a quick check
        if len(self._contents[CONSTANTS.CSV_VARS.EPOCH]) != len(
            other._contents[CONSTANTS.CSV_VARS.EPOCH]
        ):
            logger.warning(
                "Epoch columns have different lengths, layers are not compatible."
            )
            return False

        # compare the first and last epoch values as a quick check before doing a full comparison
        if (
            self._contents[CONSTANTS.CSV_VARS.EPOCH].iloc[0]
            != other._contents[CONSTANTS.CSV_VARS.EPOCH].iloc[0]
            or self._contents[CONSTANTS.CSV_VARS.EPOCH].iloc[-1]
            != other._contents[CONSTANTS.CSV_VARS.EPOCH].iloc[-1]
        ):
            logger.warning(
                "Epoch columns have different start or end times, layers are not compatible."
            )
            return False

        return all(
            self._contents[CONSTANTS.CSV_VARS.EPOCH]
            == other._contents[CONSTANTS.CSV_VARS.EPOCH]
        )

    def _convert_to_raw_epoch(self):
        if self._contents is None:
            raise ValueError("No contents loaded to convert.")

        if CONSTANTS.CSV_VARS.RAW_EPOCH in self._contents.columns:
            logger.debug("Raw epoch column already exists, skipping conversion.")
            return

        logger.debug("Converting epoch values to raw epoch format.")
        self._contents[CONSTANTS.CSV_VARS.RAW_EPOCH] = lib.cdfepoch.parse(
            np.datetime_as_string(self.get_epochs(), unit="ns").tolist()
        )

    def _write_to_cdf(self, filepath: Path, createDirectory=False) -> Path:
        logger.info(f"Writing calibration layer to CDF file: {filepath!s}")
        skeleton_cdf = cdf_to_xarray(
            str(CONSTANTS.OFFSET_SKELETON_CDF), to_datetime=False
        )

        if self._contents is None:
            if self._data_path is None:
                raise ValueError("Calibration layer has no associated path for data.")
            self._contents = self._values_from_csv(self._data_path)

        logger.debug("Converting epoch values to raw epoch format for CDF.")
        self._convert_to_raw_epoch()

        offsets_values = np.nan_to_num(
            self._contents[
                [
                    CONSTANTS.CSV_VARS.OFFSET_X,
                    CONSTANTS.CSV_VARS.OFFSET_Y,
                    CONSTANTS.CSV_VARS.OFFSET_Z,
                ]
            ],
            nan=CONSTANTS.CDF_FLOAT_FILLVAL,
        )

        epoch_data = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=self._contents[CONSTANTS.CSV_VARS.RAW_EPOCH],
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.EPOCH].attrs,
        )
        offsets_data = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH, CONSTANTS.CDF_COORDS.AXIS],
            data=offsets_values,
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.OFFSETS].attrs,
        )
        offsets_data.attrs["DEPEND_0"] = CONSTANTS.CDF_VARS.EPOCH
        timedelta_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=self._contents[CONSTANTS.CSV_VARS.TIMEDELTA],
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.TIMEDELTAS].attrs,
        )
        timedelta_var.attrs["DEPEND_0"] = CONSTANTS.CDF_VARS.EPOCH
        qf_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=self._contents[CONSTANTS.CSV_VARS.QUALITY_FLAG],
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.QUALITY_FLAG].attrs,
        )
        qf_var.attrs["DEPEND_0"] = CONSTANTS.CDF_VARS.EPOCH
        qb_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=self._contents[CONSTANTS.CSV_VARS.QUALITY_BITMASK],
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.QUALITY_BITMASK].attrs,
        )
        qb_var.attrs["DEPEND_0"] = CONSTANTS.CDF_VARS.EPOCH
        offsets_dataset = xr.Dataset(
            data_vars={
                CONSTANTS.CDF_VARS.EPOCH: epoch_data,
                CONSTANTS.CDF_VARS.OFFSETS: offsets_data,
                CONSTANTS.CDF_VARS.TIMEDELTAS: timedelta_var,
                CONSTANTS.CDF_VARS.QUALITY_FLAG: qf_var,
                CONSTANTS.CDF_VARS.QUALITY_BITMASK: qb_var,
                CONSTANTS.CDF_VARS.VALIDITY_START_DATETIME: self.validity.start,
                CONSTANTS.CDF_VARS.VALIDITY_END_DATETIME: self.validity.end,
            },
            coords={
                CONSTANTS.CDF_COORDS.AXIS: [
                    CONSTANTS.CDF_COORDS.X,
                    CONSTANTS.CDF_COORDS.Y,
                    CONSTANTS.CDF_COORDS.Z,
                ]
            },
            attrs=skeleton_cdf.attrs,
        )  # type: ignore

        offsets_dataset.attrs[CONSTANTS.CDF_ATTRS.GENERATION_DATE] = str(
            np.datetime64("now")
        )
        offsets_dataset.attrs[CONSTANTS.CDF_ATTRS.DATA_VERSION] = self.version

        offsets_dataset.attrs["Parents"] = deepcopy(self.metadata.dependencies)

        xarray_to_cdf(offsets_dataset, str(filepath), istp=True, compression=6)

        return filepath

    def set_metadata(
        self,
        dependencies: list[str],
        original_science: ScienceLayer,
        calibration_id: str,
        method: CalibrationMethod = CalibrationMethod.SUM,
    ):
        """Set the metadata for the offsets layer based on the original science layer."""
        if self._contents is None:
            raise ValueError("Offsets layer contents not loaded")

        self.validity = Validity(
            start=original_science.validity.start,
            end=original_science.validity.end,
        )

        self.metadata = CalibrationMetadata(
            dependencies=dependencies,
            science=[original_science.science_file],
            creation_timestamp=np.datetime64("now"),
            content_date=original_science.metadata.content_date,
        )
        self.id = calibration_id
        self.version = 1
        self.method = method
        self.value_type = ValueType.VECTOR
        self.sensor = original_science.sensor
        self.mission = original_science.mission

    def _load_data_file(self, path: Path) -> "CalibrationLayer":
        logger.debug(f"Loading calibration layer data from {path!s}.")
        if self._contents is not None:
            logger.warning(
                f"Existing calibration values will be overwritten with data in {path!s}."
            )

        self._contents = self._values_from_csv(path)
        return self

    def _write_to_json(self, filepath: Path, createDirectory=False):
        created = super()._write_to_json(filepath, createDirectory)
        if self._contents is not None:
            if self.metadata.data_filename is None:
                self.metadata.data_filename = Path(
                    CalibrationLayerPathHandler.from_filename(filepath)
                    .get_equivalent_data_handler()
                    .get_filename()
                )

            self._write_to_csv(
                filepath.parent / self.metadata.data_filename, createDirectory
            )
        return created

    @classmethod
    def from_file(cls, path: Path, load_contents=True) -> "CalibrationLayer":
        if path.suffix == ".csv":
            return cls._from_csv(path)
        else:
            return super().from_file(path, load_contents)

    @classmethod
    def _values_from_csv(cls, path: Path) -> pd.DataFrame:
        df = pd.read_csv(
            path, parse_dates=[CONSTANTS.CSV_VARS.EPOCH], float_precision="round_trip"
        )
        if df.empty:
            raise ValueError("CSV file is empty or does not contain valid data")
        return df

    @classmethod
    def _from_csv(cls, path: Path):
        df = cls._values_from_csv(path)

        validity = Validity(
            start=df[CONSTANTS.CSV_VARS.EPOCH].iloc[0],
            end=df[CONSTANTS.CSV_VARS.EPOCH].iloc[-1],
        )

        calibration_metadata_handler = CalibrationLayerPathHandler.from_filename(path)

        method: CalibrationMethod = (
            CalibrationMethod.from_string(calibration_metadata_handler.descriptor)
            if (
                calibration_metadata_handler and calibration_metadata_handler.descriptor
            )
            else CalibrationMethod.NOOP
        )

        instance = cls(
            id="",
            mission=Mission.IMAP,
            validity=validity,
            sensor=Sensor.MAGO,
            version=0,
            metadata=CalibrationMetadata(
                dependencies=[],
                science=[],
                data_filename=path,
                creation_timestamp=np.datetime64("now"),
            ),
            value_type=ValueType.VECTOR,
            method=method,
        )
        instance._contents = df
        instance._set_content_date_from_filepath(path)
        return instance

    @classmethod
    def create_zero_offset_layer_from_science(cls, science_layer: ScienceLayer):
        if not science_layer:
            raise ValueError(
                "Science layer must be provided to create zero offset layer."
            )

        science_layer.load_contents()
        if science_layer._contents is None:
            raise ValueError(
                "Science layer contents must be loaded to create zero offset layer."
            )

        zero_offsets_df = pd.DataFrame(
            {
                CONSTANTS.CSV_VARS.EPOCH: science_layer._contents[
                    CONSTANTS.CSV_VARS.EPOCH
                ],
                CONSTANTS.CSV_VARS.OFFSET_X: 0.0,
                CONSTANTS.CSV_VARS.OFFSET_Y: 0.0,
                CONSTANTS.CSV_VARS.OFFSET_Z: 0.0,
                CONSTANTS.CSV_VARS.TIMEDELTA: 0.0,
                CONSTANTS.CSV_VARS.QUALITY_FLAG: 0,
                CONSTANTS.CSV_VARS.QUALITY_BITMASK: 0,
            }
        )

        validity = Validity(
            start=science_layer.validity.start,
            end=science_layer.validity.end,
        )

        content_date: datetime = (
            science_layer.metadata.content_date.astype(datetime)
            if science_layer.metadata.content_date is not None
            else None
        )
        datefilename = None
        if content_date:
            calibration_handler = CalibrationLayerPathHandler(
                descriptor=CalibrationMethod.NOOP.short_name, content_date=content_date
            )
            datefilehandler = calibration_handler.get_equivalent_data_handler()
            datefilename = Path(datefilehandler.get_filename())

        metadata = CalibrationMetadata(
            dependencies=[],
            science=[science_layer.science_file] if science_layer.science_file else [],
            creation_timestamp=np.datetime64("now"),
            data_filename=datefilename,
            content_date=science_layer.metadata.content_date,
        )

        zero_offset_layer = cls(
            id="",
            mission=science_layer.mission,
            validity=validity,
            sensor=science_layer.sensor,
            version=0,
            metadata=metadata,
            value_type=ValueType.VECTOR,
            method=CalibrationMethod.NOOP,
        )
        zero_offset_layer._contents = zero_offsets_df
        return zero_offset_layer
