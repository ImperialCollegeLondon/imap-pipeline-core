"""Tests for CommandConfig."""

from unittest.mock import MagicMock

from imap_mag.config.CommandConfig import CommandConfig


class TestCommandConfig:
    def test_default_work_sub_folder_is_none(self):
        config = CommandConfig()
        assert config.work_sub_folder is None

    def test_setup_work_folder_creates_directory_when_not_exists(self, tmp_path):
        app_settings = MagicMock()
        app_settings.work_folder = tmp_path / "work"
        app_settings.disk_usage_threshold = 1.0

        config = CommandConfig(work_sub_folder="mycommand")
        result = config.setup_work_folder(app_settings)

        assert result.exists()
        assert result == tmp_path / "work" / "mycommand"

    def test_setup_work_folder_uses_sub_folder_when_provided(self, tmp_path):
        app_settings = MagicMock()
        app_settings.work_folder = tmp_path
        app_settings.disk_usage_threshold = 1.0

        config = CommandConfig(work_sub_folder="subdir")
        result = config.setup_work_folder(app_settings)

        assert result == tmp_path / "subdir"

    def test_setup_work_folder_returns_same_folder_on_second_call(self, tmp_path):
        app_settings = MagicMock()
        app_settings.work_folder = tmp_path
        app_settings.disk_usage_threshold = 1.0

        config = CommandConfig()
        first = config.setup_work_folder(app_settings)
        second = config.setup_work_folder(app_settings)

        assert first == second

    def test_setup_work_folder_without_sub_folder_uses_app_work_folder(self, tmp_path):
        app_settings = MagicMock()
        app_settings.work_folder = tmp_path
        app_settings.disk_usage_threshold = 1.0

        config = CommandConfig()
        result = config.setup_work_folder(app_settings)

        assert result == tmp_path
