import logging
from pathlib import Path
from typing import ClassVar

from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

from imap_mag.config.CommandConfig import CommandConfig
from imap_mag.config.FetchConfig import FetchBinaryConfig, FetchScienceConfig
from imap_mag.config.NestedAliasEnvSettingsSource import NestedAliasEnvSettingsSource
from imap_mag.config.PublishConfig import PublishConfig
from imap_mag.config.UploadConfig import UploadConfig

logger = logging.getLogger(__name__)


class AppSettings(BaseSettings):
    """
    Application configuration class.

    Can be configured with imap-mag-config.yaml, with ENV vars like MAG_FIELD_SUBFIELD=123 and kwargs to AppSettings(data_store="some_path")
    """

    config_file: ClassVar[str] = "imap-mag-config.yaml"
    model_config = SettingsConfigDict(
        env_nested_delimiter="_",
        env_nested_max_split=2,
        env_prefix="MAG_",
        yaml_file=config_file,
    )

    # Global settings
    work_folder: Path = Path(".work")
    data_store: Path
    packet_definition: Path

    # Command settings
    fetch_binary: FetchBinaryConfig
    fetch_science: FetchScienceConfig
    process: CommandConfig
    publish: PublishConfig
    upload: UploadConfig

    # functions
    def setup_work_folder_for_command(self, command_config: CommandConfig) -> Path:
        return command_config.setup_work_folder(self)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Let config come from secret files, the yaml file, env variables and constructor args
        # Constructor args override the settings from the ENV which overrides the YAML file which override secrets files
        return (
            # Highest priority
            init_settings,
            NestedAliasEnvSettingsSource(settings_cls),
            YamlConfigSettingsSource(settings_cls),
            file_secret_settings,
            # Lowest priority
        )
