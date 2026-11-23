"""Tests for running the Leinweber calibration via the calibrate CLI."""

from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from imap_mag.cli.calibrate import calibrate
from imap_mag.config import SaveMode
from mag_toolkit.calibration import CalibrationMethod, Sensor
from mag_toolkit.calibration.CalibrationConfig import (
    CalibrationConfig,
    ScriptedLeinweberCalibrationConfig,
)
from mag_toolkit.calibration.CalibrationDefinitions import ConfigureMidpoint

GENERATE = "imap_mag.cli.calibrate.generateLeinweberOffsets.generate_leinweber_offsets"
DATE = datetime(2026, 9, 30)


def _config(
    matlab_repo: str | None = "/path/to/matlab/repo",
    duration: str | None = "24h",
    **kwargs,
) -> str:
    return ScriptedLeinweberCalibrationConfig(
        calibration_matrix_version=9,
        input_json_file="input.json",
        matlab_repo=matlab_repo,
        duration=duration,
        **kwargs,
    ).model_dump_json()


def test_get_class_finds_leinweber_config():
    assert (
        CalibrationConfig.get_class(CalibrationMethod.SCRIPTED_LEINWEBER_CALIBRATION)
        is ScriptedLeinweberCalibrationConfig
    )


def test_calibrate_leinweber_delegates_to_leinweber_command():
    produced = [Path("offsets.csv")]
    with patch(GENERATE, return_value=produced) as mock_generate:
        result = calibrate(
            start_date=DATE,
            method=CalibrationMethod.SCRIPTED_LEINWEBER_CALIBRATION,
            sensor=Sensor.MAGI,
            configuration=_config(),
            save_mode=SaveMode.LocalAndDatabase,
            cleanup_temp_files_after_run=False,
        )

    assert result == produced
    mock_generate.assert_called_once_with(
        date=DATE,
        duration_hours=24,
        matlab_repo=Path("/path/to/matlab/repo"),
        sensors=[Sensor.MAGI],
        configure_midpoint=ConfigureMidpoint.USE_12_HRS_AFTER_PIVOT_PLATFORM_MOVEMENT,
        calibration_matrix_version=9,
        calibration_input_json_file="input.json",
        verify=True,
        save_mode=SaveMode.LocalAndDatabase,
        cleanup_temp_files_after_run=False,
        allow_pivot_platform_extrapolation=False,
    )


def test_calibrate_leinweber_passes_through_leinweber_settings():
    with patch(GENERATE, return_value=[Path("offsets.csv")]) as mock_generate:
        calibrate(
            start_date=DATE,
            method=CalibrationMethod.SCRIPTED_LEINWEBER_CALIBRATION,
            configuration=_config(
                duration="1d12h",
                configure_midpoint=ConfigureMidpoint.USE_MID_DAY,
                verify=False,
                allow_pivot_platform_extrapolation=True,
            ),
        )

    kwargs = mock_generate.call_args.kwargs
    assert kwargs["duration_hours"] == 36
    assert kwargs["configure_midpoint"] == ConfigureMidpoint.USE_MID_DAY
    assert kwargs["verify"] is False
    assert kwargs["allow_pivot_platform_extrapolation"] is True


def test_leinweber_config_rejects_invalid_duration():
    with pytest.raises(ValueError, match="Invalid duration"):
        ScriptedLeinweberCalibrationConfig(
            calibration_matrix_version=9,
            input_json_file="input.json",
            duration="not-a-duration",
        )


def test_calibrate_leinweber_runs_once_per_day_in_range():
    with patch(GENERATE, return_value=[Path("offsets.csv")]) as mock_generate:
        result = calibrate(
            start_date=DATE,
            end_date=datetime(2026, 10, 2),
            method=CalibrationMethod.SCRIPTED_LEINWEBER_CALIBRATION,
            configuration=_config(),
        )

    assert [c.kwargs["date"] for c in mock_generate.call_args_list] == [
        datetime(2026, 9, 30),
        datetime(2026, 10, 1),
        datetime(2026, 10, 2),
    ]
    assert len(result) == 3


def test_calibrate_leinweber_requires_configuration():
    with patch(GENERATE) as mock_generate, pytest.raises(ValueError, match="requires"):
        calibrate(
            start_date=DATE,
            method=CalibrationMethod.SCRIPTED_LEINWEBER_CALIBRATION,
            configuration=None,
        )
    mock_generate.assert_not_called()


def test_calibrate_leinweber_requires_matlab_repo():
    with (
        patch(GENERATE) as mock_generate,
        pytest.raises(ValueError, match="matlab_repo"),
    ):
        calibrate(
            start_date=DATE,
            method=CalibrationMethod.SCRIPTED_LEINWEBER_CALIBRATION,
            configuration=_config(matlab_repo=None),
        )
    mock_generate.assert_not_called()


def test_calibrate_leinweber_requires_duration():
    with (
        patch(GENERATE) as mock_generate,
        pytest.raises(ValueError, match="duration is required"),
    ):
        calibrate(
            start_date=DATE,
            method=CalibrationMethod.SCRIPTED_LEINWEBER_CALIBRATION,
            configuration=_config(duration=None),
        )
    mock_generate.assert_not_called()


@pytest.mark.parametrize(
    ("cli_cleanup", "config_cleanup", "expected"),
    [(True, True, True), (True, False, False), (False, True, False)],
    ids=["both-clean", "config-keeps", "cli-keeps"],
)
def test_calibrate_leinweber_keeps_temp_files_if_cli_or_config_asks(
    cli_cleanup, config_cleanup, expected
):
    with patch(GENERATE, return_value=[Path("offsets.csv")]) as mock_generate:
        calibrate(
            start_date=DATE,
            method=CalibrationMethod.SCRIPTED_LEINWEBER_CALIBRATION,
            configuration=_config(cleanup_temp_files_after_run=config_cleanup),
            cleanup_temp_files_after_run=cli_cleanup,
        )

    assert mock_generate.call_args.kwargs["cleanup_temp_files_after_run"] is expected
