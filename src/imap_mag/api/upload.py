import logging
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.api.apiUtils import (
    initialiseLoggingForCommand,
)
from imap_mag.client.sdcDataAccess import SDCDataAccess
from imap_mag.config import AppSettings
from imap_mag.io import InputManager, find_supported_provider

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
    auth_code: Annotated[
        str,
        typer.Option(
            envvar="SDC_AUTH_CODE",
            help="IMAP Science Data Centre API Key",
        ),
    ],
) -> None:
    """Upload files to the SDC."""

    if not auth_code:
        logger.critical("No SDC_AUTH_CODE API key provided")
        raise ValueError("No SDC_AUTH_CODE API key provided")

    logger.info(f"Uploading {len(files)} files: {', '.join(str(f) for f in files)}")

    settings_overrides = (
        {"fetch_science": {"api": {"auth_code": auth_code}}} if auth_code else {}
    )

    app_settings = AppSettings(**settings_overrides)  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.process)
    initialiseLoggingForCommand(work_folder)

    resolved_files: list[Path] = []
    input_manager = InputManager(app_settings.data_store)

    for file in files:
        metadata_provider = find_supported_provider(file)
        resolved_file = input_manager.get_versioned_file(
            metadata_provider, latest_version=False
        )
        resolved_files.append(resolved_file)  # type: ignore

    logger.info(
        f"Found {len(resolved_files)} files for upload: {', '.join(str(f) for f in resolved_files)}"
    )

    # Upload file to SDC.
    data_access = SDCDataAccess(
        data_dir=work_folder,
        sdc_url=app_settings.upload.api.url_base,
    )

    for file in resolved_files:
        data_access.upload(file.as_posix())

    logger.info(f"Uploaded {len(resolved_files)} files successfully.")
