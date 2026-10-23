"""Tests for building layer data file path handlers in each layer data format."""

from datetime import datetime

from imap_mag.config import AppSettings
from imap_mag.io.file import CalibrationLayerPathHandler
from mag_toolkit.calibration import CalibrationMethod, LayerDataFormat

DATE = datetime(2026, 9, 30)


class TestCreateNewDatafileHandler:
    def _handler(self) -> CalibrationLayerPathHandler:
        return CalibrationLayerPathHandler.from_method(
            method=CalibrationMethod.SET_QUALITY_AND_NAN,
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
