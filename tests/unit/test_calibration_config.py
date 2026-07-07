"""Tests for CalibrationConfig, GradiometryConfig, and SetQualityAndNaNConfig."""

import pytest
import yaml

from imap_mag.config.CalibrationConfig import (
    CalibrationConfig,
    GradiometryConfig,
    ScriptedL2CalibrationConfig,
    SetQualityAndNaNConfig,
)


class TestGradiometryConfig:
    def test_default_kappa_is_zero(self):
        config = GradiometryConfig()
        assert config.kappa == 0.0

    def test_default_sc_interference_threshold_is_zero(self):
        config = GradiometryConfig()
        assert config.sc_interference_threshold == 0.0

    def test_custom_values_are_stored(self):
        config = GradiometryConfig(kappa=0.5, sc_interference_threshold=10.0)
        assert config.kappa == 0.5
        assert config.sc_interference_threshold == 10.0


class TestSetQualityAndNaNConfig:
    def test_csv_file_is_required(self):
        with pytest.raises(Exception):
            SetQualityAndNaNConfig()

    def test_csv_file_is_stored(self):
        config = SetQualityAndNaNConfig(csv_file="quality.csv")
        assert config.csv_file == "quality.csv"


class TestCalibrationConfig:
    def test_default_config_has_empty_gradiometer(self):
        config = CalibrationConfig()
        assert config.gradiometer.kappa == 0.0
        assert config.set_quality_and_nan is None

    def test_from_file_loads_gradiometer_config(self, tmp_path):
        config_data = {"gradiometer": {"kappa": 0.25, "sc_interference_threshold": 5.0}}
        config_file = tmp_path / "cal_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        config = CalibrationConfig.from_file(config_file)

        assert config.gradiometer.kappa == 0.25
        assert config.gradiometer.sc_interference_threshold == 5.0

    def test_from_file_loads_set_quality_config(self, tmp_path):
        config_data = {
            "gradiometer": {"kappa": 0.0, "sc_interference_threshold": 0.0},
            "set_quality_and_nan": {"csv_file": "quality_rules.csv"},
        }
        config_file = tmp_path / "cal_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        config = CalibrationConfig.from_file(config_file)

        assert config.set_quality_and_nan is not None
        assert config.set_quality_and_nan.csv_file == "quality_rules.csv"

    def test_from_file_with_minimal_config_uses_defaults(self, tmp_path):
        config_data = {}
        config_file = tmp_path / "cal_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        config = CalibrationConfig.from_file(config_file)

        assert config.gradiometer.kappa == 0.0
        assert config.set_quality_and_nan is None


class TestScriptedL2CalibrationConfig:
    def test_stores_required_fields(self):
        config = ScriptedL2CalibrationConfig(
            calibration_matrix_version=8,
            input_json_file="+calibration/calibration/input.json",
        )
        assert config.calibration_matrix_version == 8
        assert config.input_json_file == "+calibration/calibration/input.json"

    def test_is_a_calibration_config_subclass(self):
        assert issubclass(ScriptedL2CalibrationConfig, CalibrationConfig)
        config = ScriptedL2CalibrationConfig(
            calibration_matrix_version=1, input_json_file="input.json"
        )
        assert isinstance(config, CalibrationConfig)
        # Inherited fields are still present with defaults.
        assert config.gradiometer.kappa == 0.0

    def test_calibration_matrix_version_is_required(self):
        with pytest.raises(Exception):
            ScriptedL2CalibrationConfig(input_json_file="input.json")

    def test_input_json_file_is_required(self):
        with pytest.raises(Exception):
            ScriptedL2CalibrationConfig(calibration_matrix_version=8)

    @pytest.mark.parametrize("empty_value", ["", "   "])
    def test_input_json_file_must_be_non_empty(self, empty_value):
        with pytest.raises(Exception):
            ScriptedL2CalibrationConfig(
                calibration_matrix_version=8, input_json_file=empty_value
            )

    def test_json_round_trip_preserves_fields(self):
        config = ScriptedL2CalibrationConfig(
            calibration_matrix_version=12, input_json_file="input.json"
        )
        restored = ScriptedL2CalibrationConfig.model_validate_json(
            config.model_dump_json()
        )
        assert restored.calibration_matrix_version == 12
        assert restored.input_json_file == "input.json"

    def test_from_file_loads_scripted_config(self, tmp_path):
        config_data = {
            "calibration_matrix_version": 9,
            "input_json_file": "+calibration/calibration/input_v009.json",
        }
        config_file = tmp_path / "scripted_config.yaml"
        config_file.write_text(yaml.dump(config_data))

        config = ScriptedL2CalibrationConfig.from_file(config_file)

        assert config.calibration_matrix_version == 9
        assert config.input_json_file == "+calibration/calibration/input_v009.json"
