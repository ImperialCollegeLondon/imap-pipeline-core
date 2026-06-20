import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from imap_mag.config import AppSettings, NestedAliasEnvSettingsSource
from imap_mag.config.CommandConfig import CommandConfig


def test_custom_env_settings_no_aliases_loads_default():
    # Set up.
    settings_cls = AppSettings()  # type: ignore
    env_vars = {}

    # Exercise.
    settings_source = NestedAliasEnvSettingsSource(settings_cls)  # type: ignore
    settings_overrides = settings_source.explode_env_vars(
        "fetch_science", AppSettings.model_fields["fetch_science"], env_vars
    )

    # Verify.
    assert not settings_overrides


def test_custom_env_settings_loads_nested_aliases():
    # Set up.
    settings_cls = AppSettings()  # type: ignore

    env_vars = {
        "imap_data_access_url": "foo_value",
        "imap_api_key": "bar_value",
    }

    # Exercise.
    settings_source = NestedAliasEnvSettingsSource(settings_cls)  # type: ignore
    settings_overrides = settings_source.explode_env_vars(
        "fetch_science", AppSettings.model_fields["fetch_science"], env_vars
    )

    # Verify.
    assert "api" in settings_overrides

    assert "url_base" in settings_overrides["api"]
    assert settings_overrides["api"]["url_base"] == "foo_value"

    assert "auth_code" in settings_overrides["api"]
    assert settings_overrides["api"]["auth_code"] == "bar_value"


def test_custom_env_settings_loads_env_names():
    # Set up.
    settings_cls = AppSettings()  # type: ignore

    env_vars = {
        "mag_fetch_science_api_url_base": "foo_value",
        "mag_fetch_science_api_auth_code": "bar_value",
    }

    # Exercise.
    settings_source = NestedAliasEnvSettingsSource(settings_cls)  # type: ignore
    settings_overrides = settings_source.explode_env_vars(
        "fetch_science", AppSettings.model_fields["fetch_science"], env_vars
    )

    # Verify.
    assert "api" in settings_overrides

    assert "url_base" in settings_overrides["api"]
    assert settings_overrides["api"]["url_base"] == "foo_value"

    assert "auth_code" in settings_overrides["api"]
    assert settings_overrides["api"]["auth_code"] == "bar_value"


def test_custom_env_settings_conflict_disregards_aliases(capsys):
    # Set up.
    settings_cls = AppSettings()  # type: ignore

    env_vars = {
        "mag_fetch_science_api_url_base": "foo_value",
        "mag_fetch_science_api_auth_code": "bar_value",
        "imap_data_access_url": "this_url_is_ignored",
        "imap_api_key": "this_code_is_ignored",
    }

    # Exercise.
    settings_source = NestedAliasEnvSettingsSource(settings_cls)  # type: ignore
    settings_overrides = settings_source.explode_env_vars(
        "fetch_science", AppSettings.model_fields["fetch_science"], env_vars
    )

    # Verify.
    assert "api" in settings_overrides

    assert "url_base" in settings_overrides["api"]
    assert settings_overrides["api"]["url_base"] == "foo_value"

    assert "auth_code" in settings_overrides["api"]
    assert settings_overrides["api"]["auth_code"] == "bar_value"

    stderr = capsys.readouterr().err

    assert (
        "Conflicting values for 'url_base': foo_value (original) and this_url_is_ignored (alias). Discarding alias value."
        in stderr
    )
    assert (
        "Conflicting values for 'auth_code': bar_value (original) and this_code_is_ignored (alias). Discarding alias value."
        in stderr
    )


def test_custom_env_settings_ignores_empty_nested_aliases():
    # Set up.
    settings_cls = AppSettings()  # type: ignore

    env_vars = {
        "imap_data_access_url": "",
        "imap_api_key": "",
    }

    # Exercise.
    settings_source = NestedAliasEnvSettingsSource(settings_cls)  # type: ignore
    settings_overrides = settings_source.explode_env_vars(
        "fetch_science", AppSettings.model_fields["fetch_science"], env_vars
    )

    # Verify.
    assert not settings_overrides


# ── CommandConfig work-folder disk space checks ───────────────────────────────


def _fake_disk_usage(used_fraction: float):
    total = 1_000_000_000
    used = int(total * used_fraction)
    return shutil.disk_usage(Path("/"))._replace(
        total=total, used=used, free=total - used
    )  # type: ignore[attr-defined]


def _app_settings(work_folder: Path, threshold: float):
    return SimpleNamespace(work_folder=work_folder, disk_usage_threshold=threshold)


def test_setup_work_folder_blocked_when_disk_full(tmp_path):
    """setup_work_folder raises OSError when disk usage meets the threshold."""
    config = CommandConfig()
    settings = _app_settings(tmp_path, threshold=0.95)

    with patch("shutil.disk_usage", return_value=_fake_disk_usage(0.95)):
        with pytest.raises(OSError, match=r"95\.0%.*threshold"):
            config.setup_work_folder(settings)


def test_setup_work_folder_allowed_when_disk_has_space(tmp_path):
    """setup_work_folder succeeds when disk usage is below the threshold."""
    config = CommandConfig()
    settings = _app_settings(tmp_path, threshold=0.95)

    with patch("shutil.disk_usage", return_value=_fake_disk_usage(0.50)):
        result = config.setup_work_folder(settings)

    assert result == tmp_path


def test_setup_work_folder_blocked_for_sub_folder(tmp_path):
    """setup_work_folder checks disk space even for nested sub-folders."""
    config = CommandConfig(work_sub_folder="science")
    settings = _app_settings(tmp_path, threshold=0.95)

    with patch("shutil.disk_usage", return_value=_fake_disk_usage(0.99)):
        with pytest.raises(OSError, match="threshold"):
            config.setup_work_folder(settings)
