"""Tests for the layer_data_format parameter (calibrate CLI + path handler)."""

from datetime import datetime
from pathlib import Path

import numpy as np

from imap_mag.cli.calibrate import gradiometry
from imap_mag.config import AppSettings
from imap_mag.io.file import CalibrationLayerPathHandler
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import (
    CalibrationMethod,
    LayerDataFormat,
    Mission,
    Sensor,
)
from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
    CalibrationMetadata,
    Validity,
    ValueType,
)
from mag_toolkit.calibration.CalibrationLayer import CalibrationLayer

DATE = datetime(2026, 9, 30)
MODULE_CALL_MATLAB = (
    "mag_toolkit.calibration.calibrators.GradiometerCalibration.call_matlab"
)


def _cal_work_folder(base: Path, date: datetime, mode: str = "norm") -> Path:
    return base / f"calibrate_{date.strftime('%Y%m%d')}_{mode}"


def _write_layer_pair_as(folder: Path, descriptor: str, fmt: LayerDataFormat) -> None:
    """Write a real JSON+companion layer pair at the exact location/format
    GradiometerCalibrationJob expects, simulating what MATLAB would produce."""
    handler = CalibrationLayerPathHandler(
        descriptor=descriptor,
        content_date=DATE,
        version=1,
        version_major=1,
    )
    df = __import__("pandas").DataFrame(
        {
            CONSTANTS.CSV_VARS.EPOCH: np.array([np.datetime64(DATE)]),
            CONSTANTS.CSV_VARS.OFFSET_X: [0.0],
            CONSTANTS.CSV_VARS.OFFSET_Y: [0.0],
            CONSTANTS.CSV_VARS.OFFSET_Z: [0.0],
            CONSTANTS.CSV_VARS.TIMEDELTA: [0.0],
            CONSTANTS.CSV_VARS.QUALITY_FLAG: [0],
            CONSTANTS.CSV_VARS.QUALITY_BITMASK: [0],
        }
    )
    layer = CalibrationLayer(
        id="",
        mission=Mission.IMAP,
        validity=Validity(start=np.datetime64(DATE), end=np.datetime64(DATE)),
        sensor=Sensor.MAGO,
        version=1,
        version_major=1,
        metadata=CalibrationMetadata(
            dependencies=[],
            science=[],
            creation_timestamp=np.datetime64("now"),
            content_date=np.datetime64(DATE),
        ),
        value_type=ValueType.VECTOR,
        method=CalibrationMethod.GRADIOMETER,
    )
    layer._contents = df
    layer.metadata.data_filename = Path(
        handler.create_new_datafile_handler(fmt).get_filename()
    )
    folder.mkdir(parents=True, exist_ok=True)
    layer.writeToFile(folder / handler.get_filename())


class TestCreateNewDatafileHandler:
    def _handler(self) -> CalibrationLayerPathHandler:
        return CalibrationLayerPathHandler.from_method(
            method=CalibrationMethod.GRADIOMETER,
            content_date=DATE,
            settings=AppSettings(),
        )

    def test_honours_explicit_csv_format(self):
        handler = self._handler()
        assert (
            handler.create_new_datafile_handler(LayerDataFormat.CSV).extension == "csv"
        )

    def test_honours_explicit_parquet_format(self):
        handler = self._handler()
        assert (
            handler.create_new_datafile_handler(LayerDataFormat.PARQUET).extension
            == "parquet"
        )


class TestCalibrateLayerDataFormatEndToEnd:
    def test_default_produces_parquet_companion(
        self, monkeypatch, temp_datastore, dynamic_work_folder
    ):
        def mock_call_matlab(command):
            _write_layer_pair_as(
                _cal_work_folder(dynamic_work_folder, DATE),
                "gradiometer-norm",
                LayerDataFormat.PARQUET,
            )

        monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)

        gradiometry(start_date=DATE, mode=ScienceMode.Normal)

        assert (
            temp_datastore
            / "calibration/layers/2026/09/imap_mag_gradiometer-norm-layer-data_20260930_v001.0001.parquet"
        ).exists()

    def test_explicit_csv_produces_csv_companion(
        self, monkeypatch, temp_datastore, dynamic_work_folder
    ):
        def mock_call_matlab(command):
            _write_layer_pair_as(
                _cal_work_folder(dynamic_work_folder, DATE),
                "gradiometer-norm",
                LayerDataFormat.CSV,
            )

        monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)

        gradiometry(
            start_date=DATE,
            mode=ScienceMode.Normal,
            layer_data_format=LayerDataFormat.CSV,
        )

        assert (
            temp_datastore
            / "calibration/layers/2026/09/imap_mag_gradiometer-norm-layer-data_20260930_v001.0001.csv"
        ).exists()
