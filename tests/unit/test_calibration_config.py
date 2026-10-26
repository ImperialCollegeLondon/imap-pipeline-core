"""Tests for CalibrationConfig, SetQualityAndNaNConfig and ScriptedL2CalibrationConfig."""

import pytest
import yaml

from mag_toolkit.calibration import CreateOffsets
from mag_toolkit.calibration.CalibrationConfig import (
    CalibrationConfig,
    ScriptedL2CalibrationConfig,
    SetQualityAndNaNConfig,
)


class TestSetQualityAndNaNConfig:
    def test_csv_file_is_required(self):
        with pytest.raises(Exception):
            SetQualityAndNaNConfig()

    def test_csv_file_is_stored(self):
        config = SetQualityAndNaNConfig(csv_file="quality.csv")
        assert config.csv_file == "quality.csv"


class TestCalibrationConfig:
    def test_from_file_loads_set_quality_and_nan_config(self, tmp_path):
        config_file = tmp_path / "cal_config.yaml"
        config_file.write_text(yaml.dump({"csv_file": "quality.csv"}))

        config = SetQualityAndNaNConfig.from_file(config_file)

        assert config.csv_file == "quality.csv"


class TestScriptedL2CalibrationConfig:
    def test_stores_required_fields(self):
        config = ScriptedL2CalibrationConfig(
            calibration_matrix_version=8,
            input_json_file="+calibration/calibration/input.json",
            matlab_repo="/path/to/matlab/repo",
        )
        assert config.calibration_matrix_version == 8
        assert config.input_json_file == "+calibration/calibration/input.json"

    def test_is_a_calibration_config_subclass(self):
        assert issubclass(ScriptedL2CalibrationConfig, CalibrationConfig)
        config = ScriptedL2CalibrationConfig(
            calibration_matrix_version=1,
            input_json_file="input.json",
            matlab_repo="/path/to/matlab/repo",
        )
        assert isinstance(config, CalibrationConfig)

    def test_calibration_matrix_version_is_required(self):
        with pytest.raises(Exception):
            ScriptedL2CalibrationConfig(
                input_json_file="input.json", matlab_repo="/path/to/matlab/repo"
            )

    def test_input_json_file_is_required(self):
        with pytest.raises(Exception):
            ScriptedL2CalibrationConfig(
                calibration_matrix_version=8,
                matlab_repo="/path/to/matlab/repo",
            )

    @pytest.mark.parametrize("empty_value", ["", "   "])
    def test_input_json_file_must_be_non_empty(self, empty_value):
        with pytest.raises(Exception):
            ScriptedL2CalibrationConfig(
                calibration_matrix_version=8,
                input_json_file=empty_value,
                matlab_repo="/path/to/matlab/repo",
            )

    def test_json_round_trip_preserves_fields(self):
        config = ScriptedL2CalibrationConfig(
            calibration_matrix_version=12,
            input_json_file="input.json",
            matlab_repo="/path/to/matlab/repo",
        )
        restored = ScriptedL2CalibrationConfig.model_validate_json(
            config.model_dump_json()
        )
        assert restored.calibration_matrix_version == 12
        assert restored.input_json_file == "input.json"
        assert restored.matlab_repo == "/path/to/matlab/repo"

    def test_from_file_loads_scripted_config(self, tmp_path):
        config_data = {
            "calibration_matrix_version": 9,
            "input_json_file": "+calibration/calibration/input_v009.json",
            "matlab_repo": "/path/to/matlab/repo",
        }
        config_file = tmp_path / "scripted_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        config = ScriptedL2CalibrationConfig.from_file(config_file)

        assert config.calibration_matrix_version == 9
        assert config.input_json_file == "+calibration/calibration/input_v009.json"
        assert config.matlab_repo == "/path/to/matlab/repo"

    def test_create_offsets_defaults_to_automatic(self):
        config = ScriptedL2CalibrationConfig(
            calibration_matrix_version=9,
            input_json_file="input.json",
            matlab_repo="/path/to/matlab/repo",
        )
        assert config.write_offsets == CreateOffsets.AUTOMATIC

    def test_create_offsets_can_be_set_yes(self):
        config = ScriptedL2CalibrationConfig(
            calibration_matrix_version=9,
            input_json_file="input.json",
            matlab_repo="/path/to/matlab/repo",
            write_offsets=CreateOffsets.ALWAYS,
        )
        assert config.write_offsets == CreateOffsets.ALWAYS

    def test_create_offsets_can_be_set_no(self):
        config = ScriptedL2CalibrationConfig(
            calibration_matrix_version=9,
            input_json_file="input.json",
            matlab_repo="/path/to/matlab/repo",
            write_offsets=CreateOffsets.NEVER,
        )
        assert config.write_offsets == CreateOffsets.NEVER
