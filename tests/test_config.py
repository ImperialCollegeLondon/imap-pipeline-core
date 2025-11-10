from imap_mag.config import AppSettings, NestedAliasEnvSettingsSource
from imap_mag.util import Environment


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


def test_override_setting_with_env_var():
    # Set up.
    data_store = "/custom/data/store"
    check_ialirt_work_subfolder = "check_ialirt"
    fetch_binary_api_url_base = "http://binary-example.com"
    fetch_binary_api_auth_code = "binary-secret"
    fetch_science_api_url_base = "http://science-example.com"
    fetch_science_api_auth_code = "science-secret"
    upload_root_path = "custom/upload/root/path"

    # Exercise.
    with Environment(
        MAG_DATA_STORE=data_store,
        MAG_CHECK_IALIRT='{"work_sub_folder": "%s"}' % check_ialirt_work_subfolder,  # noqa: UP031
        MAG_FETCH_BINARY_API_URL_BASE=fetch_binary_api_url_base,
        MAG_FETCH_BINARY_API_AUTH_CODE=fetch_binary_api_auth_code,
        MAG_FETCH_SCIENCE_API_URL_BASE=fetch_science_api_url_base,
        MAG_FETCH_SCIENCE_API_AUTH_CODE=fetch_science_api_auth_code,
        MAG_UPLOAD='{"root_path": "%s"}' % upload_root_path,  # noqa: UP031
    ):
        settings_cls = AppSettings()  # type: ignore

    # Verify.
    assert str(settings_cls.data_store) == data_store
    assert settings_cls.check_ialirt.work_sub_folder == check_ialirt_work_subfolder
    assert settings_cls.fetch_binary.api.url_base == fetch_binary_api_url_base
    assert (
        settings_cls.fetch_binary.api.auth_code.get_secret_value()  # type: ignore
        == fetch_binary_api_auth_code
    )
    assert settings_cls.fetch_science.api.url_base == fetch_science_api_url_base
    assert (
        settings_cls.fetch_science.api.auth_code.get_secret_value()  # type: ignore
        == fetch_science_api_auth_code
    )
    assert settings_cls.upload.root_path == upload_root_path
