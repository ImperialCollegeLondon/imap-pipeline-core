import logging
import shutil
from datetime import datetime
from pathlib import Path

import typer

from imap_mag.appLogging import AppLogging

logger = logging.getLogger(__name__)
globalState = {"verbose": False}


def initialiseLoggingForCommand(folder):
    # initialise all logging into the workfile
    level = "debug" if globalState["verbose"] else "info"

    logFile = Path(
        folder,
        f"{datetime.now().strftime('%Y_%m_%d-%I_%M_%S_%p')}.log",
    )
    if not AppLogging.set_up_logging(
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


def throw_error_file_not_found(source_folder: Path, filename: str) -> None:
    """Throw an error if the file is not found."""
    logger.critical(
        f"Unable to find file to process in {source_folder} with name/pattern {filename}."
    )
    raise FileNotFoundError(
        f"Unable to find file to process in {source_folder} with name/pattern {filename}."
    )


def prepareWorkFile(
    file: Path, work_folder: Path, *, throw_if_not_found: bool = False
) -> Path | None:
    logger.debug(f"Grabbing file matching {file} in {work_folder}")

    files: list[Path] = []

    source_folder = file.parent
    filename = file.name

    if not source_folder.exists():
        if throw_if_not_found:
            throw_error_file_not_found(source_folder, filename)

        logger.warning(f"Folder {source_folder} does not exist")
        return None

    # if pattern contains a %
    if "%" in filename:
        updated_file = datetime.now().strftime(filename)
        logger.info(f"Pattern contains a %, replacing {filename} with {updated_file}")
        filename = updated_file

    # list all files in the share
    for matched_file in source_folder.iterdir():
        if matched_file.is_file():
            if matched_file.match(filename):
                files.append(matched_file)

    # get the most recently modified matching file
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)

    if len(files) == 0:
        throw_error_file_not_found(source_folder, filename)

    logger.info(
        f"Found {len(files)} matching files. Select the most recent one: "
        f"{files[0].absolute().as_posix()}"
    )

    # copy the file to work_folder
    work_file = Path(work_folder, files[0].name)
    logger.debug(f"Copying {files[0]} to {work_file}")
    work_file = Path(shutil.copy2(files[0], work_folder))

    return work_file


# TODO: Need to handle configuration of calibration folder, and multiple input/output folders
