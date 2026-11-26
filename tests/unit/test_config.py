import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from imap_mag.config import AppSettings, NestedAliasEnvSettingsSource
from imap_mag.config.CommandConfig import CommandConfig
from imap_mag.util.Environment import Environment


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


# ── version_major settings ────────────────────────────────────────────────────


def test_version_major_defaults_to_1():
    """version_major has a default of 1 (also set in the project yaml)."""
    settings = AppSettings()

    assert settings.version_major == 1


def test_version_major_overridden_by_env_var():
    """MAG_VERSION_MAJOR environment variable overrides the default."""
    with Environment(MAG_VERSION_MAJOR="2"):
        settings = AppSettings()

    assert settings.version_major == 2


def test_setup_work_folder_blocked_for_sub_folder(tmp_path):
    """setup_work_folder checks disk space even for nested sub-folders."""
    config = CommandConfig(work_sub_folder="science")
    settings = _app_settings(tmp_path, threshold=0.95)

    with patch("shutil.disk_usage", return_value=_fake_disk_usage(0.99)):
        with pytest.raises(OSError, match="threshold"):
            config.setup_work_folder(settings)


# ── db_url settings ───────────────────────────────────────────────────────────

ENV_DB_URL = "postgresql+psycopg://user:pass@env-host:5432/imap"
YAML_DB_URL = "postgresql+psycopg://user:pass@yaml-host:5432/imap"
KWARG_DB_URL = "postgresql+psycopg://user:pass@kwarg-host:5432/imap"
BLOCK_LOAD = "imap_mag.db.utils.SqlAlchemyConnector.load"


@pytest.fixture
def no_db_url_env(monkeypatch):
    monkeypatch.delenv("SQLALCHEMY_URL", raising=False)
    monkeypatch.delenv("MAG_DB_URL", raising=False)


@pytest.fixture
def yaml_with_db_url(tmp_path, monkeypatch):
    """Run from a copy of the project config file that also sets db_url."""
    config = Path(AppSettings.config_file).read_text()
    (tmp_path / AppSettings.config_file).write_text(
        f"{config}\ndb_url: {YAML_DB_URL}\n"
    )
    monkeypatch.chdir(tmp_path)


def _block(url: str) -> MagicMock:
    block = MagicMock()
    block._rendered_url.render_as_string.return_value = url
    return block


@pytest.mark.usefixtures("no_db_url_env")
def test_db_url_defaults_to_none():
    assert AppSettings().db_url is None


@pytest.mark.usefixtures("no_db_url_env")
def test_db_url_read_from_sqlalchemy_url_env_var(monkeypatch):
    monkeypatch.setenv("SQLALCHEMY_URL", ENV_DB_URL)

    assert AppSettings().db_url == ENV_DB_URL


@pytest.mark.usefixtures("no_db_url_env")
def test_db_url_read_from_mag_db_url_env_var(monkeypatch):
    monkeypatch.setenv("MAG_DB_URL", ENV_DB_URL)

    assert AppSettings().db_url == ENV_DB_URL


@pytest.mark.usefixtures("no_db_url_env", "yaml_with_db_url")
def test_db_url_read_from_yaml():
    assert AppSettings().db_url == YAML_DB_URL


@pytest.mark.usefixtures("no_db_url_env", "yaml_with_db_url")
def test_db_url_env_var_overrides_yaml(monkeypatch):
    monkeypatch.setenv("SQLALCHEMY_URL", ENV_DB_URL)

    assert AppSettings().db_url == ENV_DB_URL


@pytest.mark.usefixtures("no_db_url_env")
def test_db_url_kwarg_overrides_env_var(monkeypatch):
    monkeypatch.setenv("SQLALCHEMY_URL", ENV_DB_URL)

    assert AppSettings(db_url=KWARG_DB_URL).db_url == KWARG_DB_URL


def test_db_url_password_not_in_repr():
    settings = AppSettings(db_url="postgresql://user:secret-password@host/imap")

    assert "secret-password" not in repr(settings)


@pytest.mark.usefixtures("no_db_url_env")
def test_create_applies_overrides():
    settings = AppSettings.create(db_url=KWARG_DB_URL, version_major=3)

    assert settings.db_url == KWARG_DB_URL
    assert settings.version_major == 3


@pytest.mark.usefixtures("no_db_url_env")
def test_create_outside_prefect_does_not_load_the_block():
    with patch(BLOCK_LOAD) as mock_load:
        settings = AppSettings.create()

    assert settings.db_url is None
    mock_load.assert_not_called()


@pytest.mark.usefixtures("no_db_url_env")
def test_create_for_prefect_loads_url_from_imap_database_block():
    block_url = "postgresql://user:pass@block-host:5432/imap"
    with patch(BLOCK_LOAD, return_value=_block(block_url)) as mock_load:
        settings = AppSettings.create(is_prefect=True)

    mock_load.assert_called_once_with("imap-database", _sync=True)
    assert settings.db_url == "postgresql+psycopg://user:pass@block-host:5432/imap"


@pytest.mark.usefixtures("no_db_url_env")
def test_create_for_prefect_leaves_url_unset_when_block_missing():
    with patch(BLOCK_LOAD, side_effect=ValueError("Unable to find block document")):
        settings = AppSettings.create(is_prefect=True)

    assert settings.db_url is None


@pytest.mark.usefixtures("no_db_url_env")
@pytest.mark.parametrize("env_var", ["SQLALCHEMY_URL", "MAG_DB_URL"])
def test_create_for_prefect_prefers_configured_url_to_block(monkeypatch, env_var):
    monkeypatch.setenv(env_var, ENV_DB_URL)

    with patch(BLOCK_LOAD) as mock_load:
        settings = AppSettings.create(is_prefect=True)

    assert settings.db_url == ENV_DB_URL
    mock_load.assert_not_called()


@pytest.mark.usefixtures("no_db_url_env")
def test_create_for_prefect_prefers_kwarg_to_block():
    with patch(BLOCK_LOAD) as mock_load:
        settings = AppSettings.create(is_prefect=True, db_url=KWARG_DB_URL)

    assert settings.db_url == KWARG_DB_URL
    mock_load.assert_not_called()
