"""Tests for CommandConfig and CalibrationConfig."""

import tempfile
from pathlib import Path

import pytest
import yaml

from imap_mag.config.CalibrationConfig import CalibrationConfig, GradiometryConfig, SetQualityAndNaNConfig
from imap_mag.config.CommandConfig import CommandConfig


class TestCommandConfig:
    def test_default_work_sub_folder_is_none(self):
        config = CommandConfig()
        assert config.work_sub_folder is None

    def test_setup_work_folder_creates_directory_when_not_exists(self, tmp_path):
        from unittest.mock import MagicMock

        app_settings = MagicMock()
        app_settings.work_folder = tmp_path / "work"

        config = CommandConfig(work_sub_folder="mycommand")
        result = config.setup_work_folder(app_settings)

        assert result.exists()
        assert result == tmp_path / "work" / "mycommand"

    def test_setup_work_folder_uses_sub_folder_when_provided(self, tmp_path):
        from unittest.mock import MagicMock

        app_settings = MagicMock()
        app_settings.work_folder = tmp_path

        config = CommandConfig(work_sub_folder="subdir")
        result = config.setup_work_folder(app_settings)

        assert result == tmp_path / "subdir"

    def test_setup_work_folder_returns_same_folder_on_second_call(self, tmp_path):
        from unittest.mock import MagicMock

        app_settings = MagicMock()
        app_settings.work_folder = tmp_path

        config = CommandConfig()
        first = config.setup_work_folder(app_settings)
        second = config.setup_work_folder(app_settings)

        assert first == second

    def test_setup_work_folder_without_sub_folder_uses_app_work_folder(self, tmp_path):
        from unittest.mock import MagicMock

        app_settings = MagicMock()
        app_settings.work_folder = tmp_path

        config = CommandConfig()
        result = config.setup_work_folder(app_settings)

        assert result == tmp_path


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
            SetQualityAndNaNConfig()  # no csv_file provided

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
