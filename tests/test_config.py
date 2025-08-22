from imap_mag.config import AppSettings, NestedAliasEnvSettingsSource


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
