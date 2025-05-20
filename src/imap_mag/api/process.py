import logging
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appUtils, imapProcessing
from imap_mag.api.apiUtils import (
    initialiseLoggingForCommand,
    prepareWorkFile,
)
from imap_mag.config import AppSettings, SaveMode
from imap_mag.io import IFileMetadataProvider, StandardSPDFMetadataProvider

logger = logging.getLogger(__name__)


# E.g., imap-mag process solo_L2_mag-rtn-ll-internal_20240210_V00.cdf --save-mode localanddatabase
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
    save_mode: Annotated[
        SaveMode, typer.Option(help="The mode to save the processed file")
    ] = SaveMode.LocalOnly,
) -> tuple[Path, IFileMetadataProvider]:
    """Sample processing job."""
    # TODO: semantic logging
    # TODO: handle file system/cloud files - abstraction layer needed for files
    # TODO: move shared logic to a library

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.process)
    initialiseLoggingForCommand(work_folder)

    work_file = prepareWorkFile(file, work_folder)

    if work_file is None:
        logger.critical(
            f"Unable to find a file to process in {file.parent} with name/pattern {file.name}"
        )
        raise FileNotFoundError(
            f"Unable to find a file to process in {file.parent} with name/pattern {file.name}"
        )

    file_processor = imapProcessing.dispatchFile(work_file)
    file_processor.initialize(app_settings.packet_definition)
    processed_file = file_processor.process(work_file)

    spdf_metadata = StandardSPDFMetadataProvider.from_filename(processed_file)

    if spdf_metadata is None:
        return appUtils.copyFileToDestination(processed_file, app_settings.data_store)
    else:
        output_manager = appUtils.getOutputManagerByMode(
            app_settings.data_store,
            use_database=(save_mode == SaveMode.LocalAndDatabase),
        )
        return output_manager.add_file(processed_file, spdf_metadata)
