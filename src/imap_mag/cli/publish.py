import logging
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.client.SDCDataAccess import SDCDataAccess, SDCUploadError
from imap_mag.config import AppSettings
from imap_mag.io import DatastoreFileFinder, FilePathHandlerSelector
from imap_mag.io.file import IFilePathHandler

logger = logging.getLogger(__name__)


# E.g., imap-mag publish imap_mag_l2-calibration_20251017_v004.cdf
def publish(
    files: Annotated[
        list[Path],
        typer.Argument(
            help="The file names or patterns to match for the files to publish",
            exists=False,  # can be a pattern
            file_okay=True,
            dir_okay=False,
            readable=True,
            writable=False,
        ),
    ],
) -> None:
    """Publish files to the SDC."""

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.publish)

    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    logger.info(f"Publishing {len(files)} files: {', '.join(str(f) for f in files)}")

    resolved_files: list[Path] = []
    datastore_finder = DatastoreFileFinder(app_settings.data_store)

    for file in files:
        path_handler: IFilePathHandler = FilePathHandlerSelector.find_by_path(
            file, throw_if_not_found=True
        )

        resolved_file = datastore_finder.find_matching_file(
            path_handler, throw_if_not_found=True
        )
        resolved_files.append(resolved_file)

    logger.info(
        f"Found {len(resolved_files)} files for publish: {', '.join(str(f) for f in resolved_files)}"
    )

    # Publish file to SDC.
    failed: int = 0

    data_access = SDCDataAccess(
        auth_code=app_settings.publish.api.auth_code,
        data_dir=work_folder,
        sdc_url=app_settings.publish.api.url_base,
    )

    for file in resolved_files:
        try:
            data_access.upload(file.as_posix())
        except SDCUploadError as e:
            failed += 1
            logger.warning(
                f"Failed to publish file {file}: {e}. Continuing with next file."
            )

    if failed > 0:
        logger.error(
            f"Failed to publish {failed} files. Only {len(resolved_files) - failed} files published successfully."
        )
        raise RuntimeError(f"Failed to publish {failed} files.")
    else:
        logger.info(f"Published {len(resolved_files)} files successfully.")
