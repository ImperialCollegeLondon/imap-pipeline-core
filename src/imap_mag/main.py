"""Main module."""

import logging
import os
import shutil
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Annotated

# cli
import typer

# config
import yaml

from imap_db.model import File
from mag_toolkit import CDFLoader
from mag_toolkit.calibration.CalibrationApplicator import CalibrationApplicator
from mag_toolkit.calibration.calibrationFormatProcessor import (
    CalibrationFormatProcessor,
)
from mag_toolkit.calibration.Calibrator import (
    Calibrator,
    CalibratorType,
    SpinAxisCalibrator,
    SpinPlaneCalibrator,
)

from . import DB, appConfig, appLogging, appUtils, imapProcessing
from .cli.fetchScience import FetchScience
from .client.sdcDataAccess import SDCDataAccess
from .client.webPODA import WebPODA

app = typer.Typer()
globalState = {"verbose": False}


def commandInit(config: Path) -> appConfig.AppConfig:
    # load and verify the config file
    if config is None:
        logging.critical("No config file")
        raise typer.Abort()
    if config.is_file():
        configFileDict = yaml.safe_load(open(config))
        logging.debug(
            "Config file loaded from %s with content %s: ", config, configFileDict
        )
    elif config.is_dir():
        logging.critical("Config %s is a directory, need a yml file", config)
        raise typer.Abort()
    elif not config.exists():
        logging.critical("The config at %s does not exist", config)
        raise typer.Abort()
    else:
        pass

    configFile = appConfig.AppConfig(**configFileDict)

    # set up the work folder
    if not configFile.work_folder:
        configFile.work_folder = Path(".work")

    if not os.path.exists(configFile.work_folder):
        logging.debug(f"Creating work folder {configFile.work_folder}")
        os.makedirs(configFile.work_folder)

    # initialise all logging into the workfile
    level = "debug" if globalState["verbose"] else "info"

    # TODO: the log file loation should be configurable so we can keep the logs on RDS
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


@app.command()
def hello(name: str):
    print(f"Hello {name}")


def prepareWorkFile(file, configFile) -> Path | None:
    logging.debug(f"Grabbing file matching {file} in {configFile.source.folder}")

    # get all files in \\RDS.IMPERIAL.AC.UK\rds\project\solarorbitermagnetometer\live\SO-MAG-Web\quicklooks_py\
    files = []
    folder = configFile.source.folder

    if not folder.exists():
        logging.warning(f"Folder {folder} does not exist")
        return None

    # if pattern contains a %
    if "%" in file:
        updatedFile = datetime.now().strftime(file)
        logging.info(f"Pattern contains a %, replacing '{file} with {updatedFile}")
        file = updatedFile

    # list all files in the share
    for matchedFile in folder.iterdir():
        if matchedFile.is_file():
            if matchedFile.match(file):
                files.append(matchedFile)

    # get the most recently modified matching file
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)

    if len(files) == 0:
        logging.critical(f"No files matching {file} found in {folder}")
        raise typer.Abort()

    logging.info(
        f"Found {len(files)} matching files. Select the most recent one:"
        f"{files[0].absolute().as_posix()}"
    )

    # copy the file to configFile.work_folder
    workFile = Path(configFile.work_folder, files[0].name)
    logging.debug(f"Copying {files[0]} to {workFile}")
    workFile = Path(shutil.copy2(files[0], configFile.work_folder))

    return workFile


# E.g  imap-mag process --config config.yaml solo_L2_mag-rtn-ll-internal_20240210_V00.cdf
@app.command()
def process(
    config: Annotated[Path, typer.Option()] = Path("config.yaml"),
    file: str = typer.Argument(
        help="The file name or pattern to match for the input file"
    ),
):
    """Sample processing job."""
    # TODO: semantic logging
    # TODO: handle file system/cloud files - abstraction layer needed for files
    # TODO: move shared logic to a library

    configFile: appConfig.AppConfig = commandInit(config)

    workFile = prepareWorkFile(file, configFile)

    if workFile is None:
        logging.critical(
            "Unable to find a file to process in %s", configFile.source.folder
        )
        raise typer.Abort()

    fileProcessor = imapProcessing.dispatchFile(workFile)
    fileProcessor.initialize(configFile)
    result = fileProcessor.process(workFile)

    appUtils.copyFileToDestination(result, configFile.destination)


# E.g., imap-mag fetch-binary --apid 1063 --start-date 2025-05-02 --end-date 2025-05-03
@app.command()
def fetch_binary(
    auth_code: Annotated[
        str,
        typer.Option(
            envvar="WEBPODA_AUTH_CODE",
            help="WebPODA authentication code",
        ),
    ],
    apid: Annotated[int, typer.Option(help="ApID to download")],
    start_date: Annotated[str, typer.Option(help="Start date for the download")],
    end_date: Annotated[str, typer.Option(help="End date for the download")],
    config: Annotated[Path, typer.Option()] = Path("config.yaml"),
):
    configFile: appConfig.AppConfig = commandInit(config)

    if not auth_code:
        logging.critical("No WebPODA authorization code provided")
        raise typer.Abort()

    packet: str = appUtils.getPacketFromApID(apid)
    logging.info(f"Downloading raw packet {packet} from {start_date} to {end_date}.")

    poda = WebPODA(
        auth_code,
        configFile.work_folder,
        configFile.api.webpoda_url if configFile.api else None,
    )
    result: str = poda.download(
        packet=packet,
        start_date=appUtils.convertToDatetime(start_date),
        end_date=appUtils.convertToDatetime(end_date),
    )

    appUtils.copyFileToDestination(result, configFile.destination)


class LevelEnum(str, Enum):
    level_1a = "l1a"
    level_1b = "l1b"
    level_1c = "l1c"
    level_2 = "l2"


# E.g., imap-mag fetch-science --start-date 2025-05-02 --end-date 2025-05-03
@app.command()
def fetch_science(
    auth_code: Annotated[
        str,
        typer.Option(
            envvar="SDC_AUTH_CODE",
            help="IMAP Science Data Centre API Key",
        ),
    ],
    start_date: Annotated[str, typer.Option(help="Start date for the download")],
    end_date: Annotated[str, typer.Option(help="End date for the download")],
    level: Annotated[
        LevelEnum, typer.Option(help="Level to download")
    ] = LevelEnum.level_2,
    config: Annotated[Path, typer.Option()] = Path("config-sci.yaml"),
):
    configFile: appConfig.AppConfig = commandInit(config)

    if not auth_code:
        logging.critical("No SDC_AUTH_CODE API key provided")
        raise typer.Abort()

    logging.info(f"Downloading {level} science from {start_date} to {end_date}.")

    data_access = SDCDataAccess(
        data_dir=str(configFile.work_folder),
        sdc_url=configFile.api.sdc_url if configFile.api else None,
    )

    fetch_science = FetchScience(data_access)
    files = fetch_science.download_latest_science(
        level=level.value, start_date=start_date, end_date=end_date
    )

    records = []
    for file in files:
        records.append(File(name=file.name, path=file.absolute().as_posix()))

    for file in files:
        appUtils.copyFileToDestination(file, configFile.destination)

    if configFile.destination.export_to_database:
        db = DB.DB()
        db.insert_files(records)

        logging.info(f"Downloaded {len(files)} files and saved to database")


# imap-mag calibrate --config calibration_config.yaml --method SpinAxisCalibrator imap_mag_l1b_norm-mago_20250502_v000.cdf
@app.command()
def calibrate(
    config: Annotated[Path, typer.Option()] = Path("calibration_config.yaml"),
    method: Annotated[CalibratorType, typer.Option()] = "SpinAxisCalibrator",
    input: str = typer.Argument(
        help="The file name or pattern to match for the input file"
    ),
):
    # TODO: Define specific calibration configuration
    # Using AppConfig for now to piggyback off of configuration
    # verification and work area setup
    configFile: appConfig.AppConfig = commandInit(config)

    workFile = prepareWorkFile(input, configFile)

    if workFile is None:
        logging.critical(
            "Unable to find a file to process in %s", configFile.source.folder
        )
        raise typer.Abort()

    calibrator: Calibrator

    match method:
        case CalibratorType.SPINAXIS:
            calibrator = SpinAxisCalibrator()
        case CalibratorType.SPINPLANE:
            calibrator = SpinPlaneCalibrator()

    inputData = CDFLoader.load_cdf(workFile)
    calibration = calibrator.generateCalibration(inputData)

    tempOutputFile = os.path.join(configFile.work_folder, "calibration.json")

    result = CalibrationFormatProcessor.writeToFile(calibration, tempOutputFile)

    appUtils.copyFileToDestination(result, configFile.destination)


# imap-mag apply --config calibration_application_config.yaml --calibration calibration.json imap_mag_l1a_norm-mago_20250502_v000.cdf
@app.command()
def apply(
    config: Annotated[Path, typer.Option()] = Path(
        "calibration_application_config.yaml"
    ),
    calibration: Annotated[str, typer.Option()] = "calibration.json",
    input: str = typer.Argument(
        help="The file name or pattern to match for the input file"
    ),
):
    configFile: appConfig.AppConfig = commandInit(config)

    workDataFile = prepareWorkFile(input, configFile)
    workCalibrationFile = prepareWorkFile(calibration, configFile)
    workOutputFile = os.path.join(configFile.work_folder, "l2_data.cdf")

    applier = CalibrationApplicator()

    L2_file = applier.apply(workCalibrationFile, workDataFile, workOutputFile)

    appUtils.copyFileToDestination(L2_file, configFile.destination)


@app.callback()
def main(verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False):
    if verbose:
        globalState["verbose"] = True


if __name__ == "__main__":
    app()  # pragma: no cover
