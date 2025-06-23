import logging
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.api.apiUtils import (
    initialiseLoggingForCommand,
    prepareWorkFile,
)
from imap_mag.client.sdcDataAccess import SDCDataAccess
from imap_mag.config import AppSettings

logger = logging.getLogger(__name__)


# E.g., imap-mag upload imap_mag_l2-calibration_20251017_v004.cdf
def upload(
    files: Annotated[
        list[Path],
        typer.Argument(
            help="The file names or patterns to match for the files to upload",
            exists=False,  # can be a pattern
            file_okay=True,
            dir_okay=False,
            readable=True,
            writable=False,
        ),
    ],
) -> None:
    """Process a single file."""

    logger.info(f"Uploading {len(files)} files:\n{', '.join(str(f) for f in files)}")

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.process)
    initialiseLoggingForCommand(work_folder)

    work_files: list[Path] = []

    for file in files:
        work_files.append(prepareWorkFile(file, work_folder, throw_if_not_found=True))  # type: ignore

    logger.info(
        f"Found {len(work_files)} files for upload:\n{', '.join(str(f) for f in work_files)}"
    )

    # Upload file to SDC.
    data_access = SDCDataAccess(
        data_dir=work_folder,
        sdc_url=app_settings.upload.api.url_base,
    )

    for file in work_files:
        data_access.upload(file.as_posix())

    logger.info(f"Uploaded {len(work_files)} files successfully.")
