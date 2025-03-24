import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

import typer
import yaml

from imap_mag import appConfig, appLogging

logger = logging.getLogger(__name__)
globalState = {"verbose": False}


def commandInit(config: Path | None) -> appConfig.AppConfig:
    # load and verify the config file
    if config is None:
        logger.critical("No config file")
        raise typer.Abort()
    if config.is_file():
        configFileDict = yaml.safe_load(open(config))
        logger.debug(
            "Config file loaded from %s with content %s: ", config, configFileDict
        )
    elif config.is_dir():
        logger.critical("Config %s is a directory, need a yml file", config)
        raise typer.Abort()
    elif not config.exists():
        logger.critical("The config at %s does not exist", config)
        raise typer.Abort()
    else:
        pass

    configFile = appConfig.AppConfig(**configFileDict)

    # set up the work folder
    if not configFile.work_folder:
        configFile.work_folder = Path(".work")

    if not os.path.exists(configFile.work_folder):
        logger.debug(f"Creating work folder {configFile.work_folder}")
        os.makedirs(configFile.work_folder)

    # initialise all logging into the workfile
    level = "debug" if globalState["verbose"] else "info"

    # TODO: the log file location should be configurable so we can keep the logs on RDS
    # Or maybe just ship them there after the fact? Or log to both?
    logFile = Path(
        configFile.work_folder,
        f"{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}.log",
    )
    if not appLogging.set_up_logging(
        console_log_output="stdout",
        console_log_level=level,
        console_log_color=True,
        logfile_file=logFile,
        logfile_log_level="debug",
        logfile_log_color=False,
        log_line_template="%(color_on)s[%(asctime)s] [%(levelname)-8s] %(message)s%(color_off)s",
        console_log_line_template="%(color_on)s%(message)s%(color_off)s",
    ):
        print("Failed to set up logging, aborting.")
        raise typer.Abort()

    return configFile


def prepareWorkFile(file: Path | str, configFile) -> Path | None:
    logger.debug(f"Grabbing file matching {file} in {configFile.source.folder}")

    if isinstance(file, Path):
        file = file.as_posix()

    files = []
    folder = configFile.source.folder

    if not folder.exists():
        logger.warning(f"Folder {folder} does not exist")
        return None

    # if pattern contains a %
    if "%" in file:
        updatedFile = datetime.now().strftime(file)
        logger.info(f"Pattern contains a %, replacing '{file} with {updatedFile}")
        file = updatedFile

    # list all files in the share
    for matchedFile in folder.iterdir():
        if matchedFile.is_file():
            if matchedFile.match(file):
                files.append(matchedFile)

    # get the most recently modified matching file
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)

    if len(files) == 0:
        logger.critical(f"No files matching {file} found in {folder}")
        raise FileNotFoundError(f"No files matching {file} found in {folder}")

    logger.info(
        f"Found {len(files)} matching files. Select the most recent one: "
        f"{files[0].absolute().as_posix()}"
    )

    # copy the file to configFile.work_folder
    workFile = Path(configFile.work_folder, files[0].name)
    logger.debug(f"Copying {files[0]} to {workFile}")
    workFile = Path(shutil.copy2(files[0], configFile.work_folder))

    return workFile
