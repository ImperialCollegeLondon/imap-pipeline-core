import os
from pathlib import Path, PosixPath, WindowsPath

import imap_mag.appConfig as appConfig
import pytest
import yaml
from imap_mag import appLogging


@pytest.fixture(autouse=True)
def enableLogging():
    appLogging.set_up_logging(
        console_log_output="stdout",
        console_log_level="debug",
        console_log_color=True,
        logfile_file="debug",
        logfile_log_level="debug",
        logfile_log_color=False,
        log_line_template="%(color_on)s[%(asctime)s] [%(levelname)-8s] %(message)s%(color_off)s",
        console_log_line_template="%(color_on)s%(message)s%(color_off)s",
    )
    yield


@pytest.fixture(autouse=True)
def tidyDataFolders():
    os.system("rm -rf .work")
    os.system("rm -rf output/*")
    yield


def create_serialize_config(
    *,
    source: Path = Path("."),
    destination_folder: Path = Path("output"),
    destination_file: str = "results.csv",
    webpoda_url: str | None = None,
    sdc_url: str | None = None,
    export_to_database: bool = False,
) -> tuple[appConfig.AppConfig, str]:
    """Create and serialize a configuration object."""

    config = appConfig.AppConfig(
        source=appConfig.Source(folder=source),
        destination=appConfig.Destination(
            folder=destination_folder,
            filename=destination_file,
            export_to_database=export_to_database,
        ),
        api=appConfig.API(webpoda_url=webpoda_url, sdc_url=sdc_url),
    )

    if not os.path.exists(config.work_folder):
        os.makedirs(config.work_folder)

    config_file = os.path.join(config.work_folder, "config-test.yaml")

    with open(config_file, "w") as f:
        yaml.add_representer(
            PosixPath, lambda dumper, data: dumper.represent_str(str(data))
        )
        yaml.add_representer(
            WindowsPath, lambda dumper, data: dumper.represent_str(str(data))
        )
        yaml.dump(config.model_dump(by_alias=True), f)

    return (config, config_file)
