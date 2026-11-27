import logging
from pathlib import Path
from typing import Any, ClassVar, Self

from pydantic import AliasChoices, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)

from imap_mag.config.CalibrationCommandConfig import (
    DEFAULT_METAKERNEL_FILE_TYPES,
    CalibrationCommandConfig,
)
from imap_mag.config.CheckIALiRTConfig import CheckIALiRTConfig
from imap_mag.config.CommandConfig import CommandConfig
from imap_mag.config.ConvertCommandConfig import ConvertCommandConfig
from imap_mag.config.DatastoreCleanupConfig import DatastoreCleanupConfig
from imap_mag.config.DeleteDatabaseRowsConfig import DeleteDatabaseRowsConfig
from imap_mag.config.FetchConfig import (
    FetchBinaryConfig,
    FetchIALiRTConfig,
    FetchScienceConfig,
    FetchSOLAR1andACEConfig,
    FetchSpiceConfig,
    FetchWebTCADLaTiSConfig,
)
from imap_mag.config.FileAnalysisConfig import FileAnalysisConfig
from imap_mag.config.HeliopuddleConfig import HeliopuddleConfig
from imap_mag.config.NestedAliasEnvSettingsSource import NestedAliasEnvSettingsSource
from imap_mag.config.PostgresUploadConfig import PostgresUploadConfig
from imap_mag.config.PublishConfig import PublishConfig
from imap_mag.config.QuicklookConfig import QuicklookConfig
from imap_mag.config.UploadConfig import UploadConfig

logger = logging.getLogger(__name__)


class AppSettings(BaseSettings):
    """
    Application configuration class.

    Can be configured with imap-mag-config.yaml, with ENV vars like MAG_FIELD_SUBFIELD=123 and kwargs to AppSettings(data_store="some_path")

    Build it once at the program entrypoint (CLI command or flow start), with
    AppSettings.create(), and pass the instance to everything that needs settings.
    """

    config_file: ClassVar[str] = "imap-mag-config.yaml"
    model_config = SettingsConfigDict(
        env_nested_delimiter="_",
        env_nested_max_split=2,
        env_prefix="MAG_",
        yaml_file=config_file,
        extra="ignore",
        # Lets db_url be set by its field name as well as by its env var aliases.
        validate_by_name=True,
    )

    # Global settings
    work_folder: Path = Path(".work")  # type: ignore
    data_store: Path
    packet_definition: Path
    disk_usage_threshold: float = 0.95
    version_major: int = 1

    # IMAP database URL. Read from the SQLALCHEMY_URL (or MAG_DB_URL) env var, db_url
    # in the YAML (a local SQLite file by default) or the db_url kwarg. In Prefect
    # flows, AppSettings.create(is_prefect=True) takes it from the imap-database
    # SqlAlchemyConnector block instead whenever that block exists.
    db_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices("SQLALCHEMY_URL", "MAG_DB_URL"),
        repr=False,
    )

    # SPICE kernel types to include when generating a metakernel. Shared by the
    # calibration applicator and the scripted L2 calibration.
    metakernel_file_types: list[str] = DEFAULT_METAKERNEL_FILE_TYPES

    # Command settings
    check_ialirt: CheckIALiRTConfig
    fetch_binary: FetchBinaryConfig
    fetch_webtcad: FetchWebTCADLaTiSConfig
    fetch_ialirt: FetchIALiRTConfig
    fetch_science: FetchScienceConfig
    fetch_spice: FetchSpiceConfig
    fetch_solar1_ace: FetchSOLAR1andACEConfig
    plot_ialirt: QuicklookConfig
    apply: CommandConfig
    calibrate: CalibrationCommandConfig = CalibrationCommandConfig()
    convert: ConvertCommandConfig = ConvertCommandConfig()
    process: CommandConfig
    publish: PublishConfig
    upload: UploadConfig
    postgres_upload: PostgresUploadConfig
    datastore_cleanup: DatastoreCleanupConfig
    database_delete_rows: DeleteDatabaseRowsConfig
    file_analysis: FileAnalysisConfig = Field(default_factory=FileAnalysisConfig)
    heliopuddle: HeliopuddleConfig = Field(default_factory=HeliopuddleConfig)

    @classmethod
    def create(cls, is_prefect: bool = False, **overrides: Any) -> Self:
        """Build the settings for a program entrypoint.

        Args:
            is_prefect: True when running in a Prefect flow. The database URL then
                comes from the Prefect SqlAlchemyConnector block named imap-database
                when that block exists, overriding any other setting.
            **overrides: Settings that take priority over the env vars and YAML file.

        Returns:
            The application settings.
        """
        settings = cls(**overrides)
        if is_prefect:
            # Imported here so that loading settings does not import Prefect.
            from imap_mag.db.utils import load_database_url
            from prefect_server.constants import PREFECT_CONSTANTS

            block_name = PREFECT_CONSTANTS.IMAP_DATABASE_BLOCK_NAME
            block_url = load_database_url(block_name)
            if block_url:
                settings.db_url = block_url
            elif settings.db_url:
                logger.info(
                    f"No {block_name} Prefect block: using the configured database URL."
                )
            else:
                logger.warning(
                    f"No database URL in the {block_name} Prefect block, SQLALCHEMY_URL or the config."
                )
        return settings

    # functions
    def setup_work_folder_for_command(
        self,
        command_config: CommandConfig,
        name_context: dict[str, str] | None = None,
    ) -> Path:
        return command_config.setup_work_folder(self, name_context)

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
