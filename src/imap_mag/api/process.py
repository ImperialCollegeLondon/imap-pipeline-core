import logging
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appConfig, appUtils, imapProcessing
from imap_mag.api.apiUtils import commandInit, prepareWorkFile
from imap_mag.outputManager import IFileMetadataProvider, StandardSPDFMetadataProvider

logger = logging.getLogger(__name__)


# E.g., imap-mag process --config config.yaml solo_L2_mag-rtn-ll-internal_20240210_V00.cdf
def process(
    file: Annotated[
        Path,
        typer.Argument(
            help="The file name or pattern to match for the input file",
            exists=False,  # can be a pattern
            file_okay=True,
            dir_okay=False,
            readable=True,
            writable=False,
        ),
    ],
    config: Annotated[Path, typer.Option()] = Path("config.yaml"),
) -> tuple[Path, IFileMetadataProvider]:
    """Sample processing job."""
    # TODO: semantic logging
    # TODO: handle file system/cloud files - abstraction layer needed for files
    # TODO: move shared logic to a library

    configFile: appConfig.AppConfig = commandInit(config)
    workFile = prepareWorkFile(file, configFile)

    if workFile is None:
        logger.critical(
            f"Unable to find a file to process in {configFile.source.folder} with name/pattern {file!s}"
        )
        raise FileNotFoundError(
            f"Unable to find a file to process in {configFile.source.folder} with name/pattern {file!s}"
        )

    fileProcessor = imapProcessing.dispatchFile(workFile)
    fileProcessor.initialize(configFile)
    processedFile = fileProcessor.process(workFile)

    spdf_metadata = StandardSPDFMetadataProvider.from_filename(processedFile)

    if spdf_metadata is None:
        return appUtils.copyFileToDestination(processedFile, configFile.destination)
    else:
        output_manager = appUtils.getOutputManager(configFile.destination)
        return output_manager.add_file(processedFile, spdf_metadata)
