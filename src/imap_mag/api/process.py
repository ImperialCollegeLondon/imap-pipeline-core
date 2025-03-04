import logging
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appConfig, appUtils, imapProcessing
from imap_mag.api.apiUtils import commandInit, prepareWorkFile


# E.g., imap-mag process --config config.yaml solo_L2_mag-rtn-ll-internal_20240210_V00.cdf
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
