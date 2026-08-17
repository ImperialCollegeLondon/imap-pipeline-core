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

from imap_mag import get_version
from imap_mag.config import AppSettings
from imap_mag.io.file import CalibrationLayerPathHandler, IFilePathHandler
from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
    CalibrationMetadata,
    CalibrationMethod,
    FileType,
    LayerDataFormat,
    Mission,
    Sensor,
    ValueType,
)
from mag_toolkit.calibration.Layer import Layer, Validity
from mag_toolkit.calibration.ScienceLayer import ScienceLayer

logger = logging.getLogger(__name__)


def _format_epoch_as_nanosecond_string(epoch: pd.Series) -> pd.Series:
    """Format a datetime64[ns] series as ISO 8601 text with a fixed 9-digit
    (nanosecond) fractional-second part, so text-based formats (CSV) retain the
    same precision as native binary formats (Parquet)."""
    sub_second_ns = epoch.dt.microsecond.astype(
        "int64"
    ) * 1000 + epoch.dt.nanosecond.astype("int64")
    return (
        epoch.dt.strftime("%Y-%m-%dT%H:%M:%S")
        + "."
        + sub_second_ns.astype(str).str.zfill(9)
    )


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

        # pandas' to_csv(date_format=...) only supports %f (microseconds), which
        # would silently truncate any sub-microsecond precision in the epoch
        # column. Format it manually with a fixed 9-digit (nanosecond) fractional
        # part instead, so CSV round-trips with the same precision as Parquet.
        df = self._contents
        epoch_col = CONSTANTS.CSV_VARS.EPOCH
        if epoch_col in df.columns and pd.api.types.is_datetime64_any_dtype(
            df[epoch_col]
        ):
            df = df.copy()
            df[epoch_col] = _format_epoch_as_nanosecond_string(df[epoch_col])

        df.to_csv(
            filepath,
            index=False,
            header=True,
        )
        return filepath

    def _write_to_parquet(self, filepath: Path, createDirectory=False):
        if self._contents is None:
            raise ValueError("No contents loaded to write to Parquet.")
        if createDirectory:
            filepath.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Writing calibration layer Parquet data to {filepath!s}.")

        # The epoch column is stored as a native pandas datetime64[ns] column via
        # pyarrow, which keeps full nanosecond precision losslessly (unlike CSV's
        # text formatting, which truncates to microseconds). Other numeric columns
        # are written as their native dtype for the same reason.
        self._contents.to_parquet(
            filepath,
            index=False,
            engine="pyarrow",
            compression="zstd",
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
            self._load_data_file(self._data_path)

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
            data=self._contents[CONSTANTS.CSV_VARS.QUALITY_FLAG].astype(int),
            attrs=skeleton_cdf[CONSTANTS.CDF_VARS.QUALITY_FLAG].attrs,
        )
        qf_var.attrs["DEPEND_0"] = CONSTANTS.CDF_VARS.EPOCH
        qb_var = xr.Variable(
            dims=[CONSTANTS.CDF_VARS.EPOCH],
            data=self._contents[CONSTANTS.CSV_VARS.QUALITY_BITMASK].astype(int),
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
        if self.version_major > 0:
            data_version = f"v{self.version_major:03d}.{self.version:04d}"
        else:
            data_version = f"v{self.version:03d}"
        offsets_dataset.attrs[CONSTANTS.CDF_ATTRS.DATA_VERSION] = data_version

        offsets_dataset.attrs["Parents"] = deepcopy(self.metadata.dependencies)

        xarray_to_cdf(offsets_dataset, str(filepath), istp=True, compression=6)

        return filepath

    def set_metadata(
        self,
        dependencies: list[str],
        original_science: ScienceLayer,
        calibration_id: str,
        method: CalibrationMethod = CalibrationMethod.SUM,
        version_major: int = 0,
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
        self.version_major = version_major
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

        if path.suffix == ".parquet":
            self._contents = self._values_from_parquet(path)
        elif path.suffix == ".cdf":
            self._contents = self._values_from_cdf(path)
        else:
            self._contents = self._values_from_csv(path)
        return self

    def _write_to_json(self, filepath: Path, createDirectory=False):

        return self.save_calibration_layer(
            filepath, createDirectory, save_contents=True
        )

    def save_calibration_layer(self, filepath, createDirectory, save_contents=True):
        if self._contents is not None:
            if self.metadata.data_filename is None:
                # No format was specified for this write; default to parquet.
                # Callers that care about the companion format should set
                # metadata.data_filename themselves before calling write_to_file.
                self.metadata.data_filename = Path(
                    CalibrationLayerPathHandler.from_filename(filepath)
                    .create_new_datafile_handler(LayerDataFormat.PARQUET)
                    .get_filename()
                )
            data_file_path = filepath.parent / self.metadata.data_filename
            if save_contents:
                if data_file_path.suffix.lstrip(".") == LayerDataFormat.PARQUET.value:
                    self._write_to_parquet(data_file_path, createDirectory)
                else:
                    self._write_to_csv(data_file_path, createDirectory)

        data_file_path = filepath.parent / self.metadata.data_filename
        if self.metadata.data_hash is None and data_file_path.exists():
            self.metadata.data_hash = IFilePathHandler.default_file_hash(data_file_path)
            logger.debug(
                f"Wrote data hash for {data_file_path!s} as {self.metadata.data_hash!s}."
            )

        dependency = f"imap-pipeline-core version {get_version()}"
        if self.metadata.dependencies is None:
            self.metadata.dependencies = []
        if dependency not in self.metadata.dependencies:
            self.metadata.dependencies.append(dependency)

        created = super()._write_to_json(filepath, createDirectory)

        self._local_file_path = created

        return created

    @staticmethod
    def is_self_contained_format(extension: str) -> bool:
        """True for formats (currently just CDF) that embed all layer data and
        metadata into a single file, rather than a JSON + companion data-file pair."""
        return extension == "cdf"

    def get_data_file_type(self) -> FileType:
        """Return the format of this layer's actual data: its own file for a
        self-contained format (e.g. CDF), or the companion csv/parquet file for
        a paired JSON layer.

        Relies on ``metadata.data_filename`` always pointing at the file that
        holds the data — the companion for a paired layer, or the layer's own
        file for a self-contained one (see ``_build_from_values``).
        """
        if self.metadata.data_filename is None:
            raise ValueError(
                "Layer has no data_filename set; cannot determine its data file type."
            )
        return FileType(Path(self.metadata.data_filename).suffix.lstrip("."))

    def get_datafile_path(self, local_metadata_path: Path | None = None) -> Path | None:
        """Return the path of the companion data file (CSV or Parquet) for a JSON layer, or the layer's own path for a self-contained format (CDF).
        The path will be relative to the JSON layer's location unless ``local_metadata_path`` is providedwhen it will be relative to that instead.
        """
        if self.is_self_contained_format(self.get_data_file_type().value):
            return (
                local_metadata_path
                if local_metadata_path is not None
                else self._local_file_path
            )

        if self.metadata.data_filename is None:
            return None

        if local_metadata_path is None and self._local_file_path is None:
            raise ValueError(
                "Cannot determine companion data file path: both local_metadata_path and _local_file_path are None."
            )

        parent_folder = (
            local_metadata_path.parent
            if local_metadata_path is not None
            else self._local_file_path.parent
        )

        return parent_folder / Path(self.metadata.data_filename).name

    def prepare_metadata_for_output_format(
        self,
        output_handler: CalibrationLayerPathHandler,
        layer_data_format: LayerDataFormat | None,
    ) -> Path | None:
        """Update ``metadata.data_filename``/``data_hash`` to match
        ``output_handler``'s target format, ready for
        ``write_to_file(work_folder / output_handler.get_filename())``.

        Clears the companion reference entirely for self-contained formats
        (CDF, in which case ``layer_data_format`` is unused and may be
        ``None``); otherwise ``layer_data_format`` must be provided and is
        used to build the equivalent companion data file handler, clearing
        any stale hash so it is recomputed for the new content.

        Returns the companion data file's filename, or ``None`` for
        self-contained formats.
        """
        if self.is_self_contained_format(output_handler.extension):
            self.metadata.data_filename = None
            self.metadata.data_hash = None
            return None

        assert layer_data_format is not None, (
            "layer_data_format is required when the output format is not self-contained"
        )
        data_handler = output_handler.create_new_datafile_handler(layer_data_format)
        companion_filename = Path(data_handler.get_filename())
        self.metadata.data_filename = companion_filename
        self.metadata.data_hash = None
        return companion_filename

    def update_file_contents_based_on_version(
        self, handler: CalibrationLayerPathHandler, source_file: Path
    ) -> Path:
        """Rewrite this layer's data_filename to match handler's current version,
        if the datastore assigned a different version than source_file was
        originally generated at (e.g. v001 -> v002 because v001 already exists
        with different content).

        Returns source_file unchanged if no rewrite is needed, otherwise a new
        temporary file the caller must delete.
        """
        current = (
            Path(self.metadata.data_filename).name
            if self.metadata.data_filename
            else None
        )
        if current is None:
            return source_file

        # Preserve the companion's actual current format (csv/parquet) rather
        # than guessing — we are re-versioning an existing file, not creating
        # a new one, so its format is already fixed.
        current_format = LayerDataFormat(Path(current).suffix.lstrip("."))
        expected_data_filename = handler.create_new_datafile_handler(
            current_format
        ).get_filename()

        if current == expected_data_filename:
            return source_file  # already correct — no rewrite needed

        self.metadata.data_filename = Path(expected_data_filename)
        new_version_path = source_file.parent / handler.get_filename()
        self.write_to_file(new_version_path)

        logger.debug(
            f"Rewrote {source_file.name} data_filename from {current!r} to "
            f"{expected_data_filename!r} in {new_version_path.name}."
        )
        return new_version_path

    @classmethod
    def from_file(cls, path: Path, load_contents=True) -> "CalibrationLayer":
        if path.suffix == ".csv":
            return cls._from_csv(path)
        elif path.suffix == ".parquet":
            return cls._from_parquet(path)
        elif path.suffix == ".cdf":
            return cls._from_cdf(path)
        else:
            return super().from_file(path, load_contents)

    @classmethod
    def _validate_contents(cls, df: pd.DataFrame, path: Path) -> pd.DataFrame:
        if df.columns.empty:
            raise ValueError(f"Layer data file '{path.name}' is empty or invalid")

        # NaN is no longer valid in quality_flag or quality_bitmask columns.
        # Use 0 for no-op, positive to set bits, negative to clear bits.
        for col in [
            CONSTANTS.CSV_VARS.QUALITY_FLAG,
            CONSTANTS.CSV_VARS.QUALITY_BITMASK,
        ]:
            if col in df.columns and df[col].isna().any():
                raise ValueError(
                    f"Layer file '{path.name}' contains NaN/blank values in column '{col}'. "
                    f"Use 0 for no-op, a positive integer to set bits, "
                    f"or a negative integer to clear bits."
                )

        return df

    @classmethod
    def _values_from_csv(cls, path: Path) -> pd.DataFrame:
        df = pd.read_csv(
            path, parse_dates=[CONSTANTS.CSV_VARS.EPOCH], float_precision="round_trip"
        )
        return cls._validate_contents(df, path)

    @classmethod
    def _values_from_parquet(cls, path: Path) -> pd.DataFrame:
        df = pd.read_parquet(path, engine="pyarrow")

        # MATLAB writes the epoch column as text (to avoid parquetwrite's native
        # datetime round-trip truncating to microsecond precision); Python writes
        # it as a native datetime64[ns] column. Normalise both to datetime64[ns].
        epoch_col = CONSTANTS.CSV_VARS.EPOCH
        if epoch_col in df.columns and not pd.api.types.is_datetime64_any_dtype(
            df[epoch_col]
        ):
            df[epoch_col] = pd.to_datetime(df[epoch_col])

        return cls._validate_contents(df, path)

    @classmethod
    def _build_from_values(cls, df: pd.DataFrame, path: Path) -> "CalibrationLayer":
        validity = (
            Validity(
                start=df[CONSTANTS.CSV_VARS.EPOCH].iloc[0],
                end=df[CONSTANTS.CSV_VARS.EPOCH].iloc[-1],
            )
            if not df.empty
            else Validity(start=np.datetime64("NaT"), end=np.datetime64("NaT"))
        )

        calibration_metadata_handler = CalibrationLayerPathHandler.from_filename(path)

        method = CalibrationMethod.NOOP
        if calibration_metadata_handler and calibration_metadata_handler.descriptor:
            # descriptor is "{method}" or "{method}-{mode}" (e.g. "quality-norm");
            # try the full descriptor first, then just the method portion before
            # the first hyphen, since CalibrationMethod.short_name never itself
            # contains a hyphen.
            descriptor = calibration_metadata_handler.descriptor
            for candidate in (descriptor, descriptor.split("-", 1)[0]):
                try:
                    method = CalibrationMethod.from_string(candidate)
                    break
                except ValueError:
                    continue

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
            value_type=ValueType.VECTOR
            if not df.empty
            else ValueType.BOUNDARY_CHANGES_ONLY,
            method=method,
        )
        instance._contents = df
        instance._set_content_date_from_filepath(path)
        return instance

    @classmethod
    def _from_csv(cls, path: Path):
        return cls._build_from_values(cls._values_from_csv(path), path)

    @classmethod
    def _from_parquet(cls, path: Path):
        return cls._build_from_values(cls._values_from_parquet(path), path)

    @classmethod
    def _values_from_cdf(cls, path: Path) -> pd.DataFrame:
        logger.info(f"Reading calibration layer CDF data from {path!s}.")
        dataset = cdf_to_xarray(str(path), to_datetime=False)

        epoch = lib.cdfepoch.to_datetime(dataset[CONSTANTS.CDF_VARS.EPOCH].values)
        offsets = dataset[CONSTANTS.CDF_VARS.OFFSETS].values

        df = pd.DataFrame(
            {
                CONSTANTS.CSV_VARS.EPOCH: pd.to_datetime(epoch),
                CONSTANTS.CSV_VARS.OFFSET_X: offsets[:, 0],
                CONSTANTS.CSV_VARS.OFFSET_Y: offsets[:, 1],
                CONSTANTS.CSV_VARS.OFFSET_Z: offsets[:, 2],
                CONSTANTS.CSV_VARS.TIMEDELTA: dataset[
                    CONSTANTS.CDF_VARS.TIMEDELTAS
                ].values,
                CONSTANTS.CSV_VARS.QUALITY_FLAG: dataset[
                    CONSTANTS.CDF_VARS.QUALITY_FLAG
                ].values.astype(int),
                CONSTANTS.CSV_VARS.QUALITY_BITMASK: dataset[
                    CONSTANTS.CDF_VARS.QUALITY_BITMASK
                ].values.astype(int),
            }
        )

        fill_mask = df[
            [
                CONSTANTS.CSV_VARS.OFFSET_X,
                CONSTANTS.CSV_VARS.OFFSET_Y,
                CONSTANTS.CSV_VARS.OFFSET_Z,
            ]
        ].isin([CONSTANTS.CDF_FLOAT_FILLVAL])
        df[
            [
                CONSTANTS.CSV_VARS.OFFSET_X,
                CONSTANTS.CSV_VARS.OFFSET_Y,
                CONSTANTS.CSV_VARS.OFFSET_Z,
            ]
        ] = df[
            [
                CONSTANTS.CSV_VARS.OFFSET_X,
                CONSTANTS.CSV_VARS.OFFSET_Y,
                CONSTANTS.CSV_VARS.OFFSET_Z,
            ]
        ].mask(fill_mask, np.nan)

        return cls._validate_contents(df, path)

    @classmethod
    def _from_cdf(cls, path: Path):
        layer = cls._build_from_values(cls._values_from_cdf(path), path)
        layer._local_file_path = path
        return layer

    @classmethod
    def create_zero_offset_layer_from_science(
        cls,
        science_layer: ScienceLayer,
        settings: AppSettings = AppSettings(),
        layer_data_format: LayerDataFormat | None = None,
    ) -> "CalibrationLayer":
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
            calibration_handler = CalibrationLayerPathHandler.from_method(
                method=CalibrationMethod.NOOP,
                content_date=content_date,
                settings=settings,
            )
            datefilehandler = calibration_handler.create_new_datafile_handler(
                layer_data_format
                if layer_data_format is not None
                else LayerDataFormat.PARQUET
            )
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
