"""Unit tests for calibrate_convert_flow."""

from datetime import date, datetime
from unittest.mock import patch

from imap_mag.cli.apply import FileType
from mag_toolkit.calibration import ConversionStrategy
from prefect_server.calibrateConvertFlow import calibrate_convert_flow
from prefect_server.constants import PREFECT_CONSTANTS


class TestCalibrateConvertFlow:
    def test_is_registered_with_correct_flow_name(self):
        assert calibrate_convert_flow.name == (
            PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE_CONVERT
        )

    def test_delegates_to_convert_with_expected_arguments(self):
        with patch(
            "prefect_server.calibrateConvertFlow.convert", return_value=[]
        ) as mock_convert:
            calibrate_convert_flow.fn(
                input_layers=["*noop*"],
                start_date=date(2026, 1, 16),
                end_date=date(2026, 1, 17),
                output_layer_data_format=FileType.CSV,
                output_layer_versioning_strategy=ConversionStrategy.CREATE_NEW_VERSION,
            )

        mock_convert.assert_called_once()
        kwargs = mock_convert.call_args.kwargs
        assert kwargs["input_layers"] == ["*noop*"]
        assert kwargs["start_date"] == datetime(2026, 1, 16)
        assert kwargs["end_date"] == datetime(2026, 1, 17)
        assert kwargs["output_layer_data_format"] == FileType.CSV
        assert kwargs["output_layer_versioning_strategy"] == (
            ConversionStrategy.CREATE_NEW_VERSION
        )

    def test_defaults_output_format_to_parquet(self):
        with patch(
            "prefect_server.calibrateConvertFlow.convert", return_value=[]
        ) as mock_convert:
            calibrate_convert_flow.fn(input_layers=["*noop*"])

        kwargs = mock_convert.call_args.kwargs
        assert kwargs["output_layer_data_format"] == FileType.PARQUET
        assert kwargs["output_layer_versioning_strategy"] == (
            ConversionStrategy.OVERWRITE
        )
        assert kwargs["start_date"] is None
        assert kwargs["end_date"] is None
