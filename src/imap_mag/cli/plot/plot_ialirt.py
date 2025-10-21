import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from imap_mag import appUtils
from imap_mag.cli.cliUtils import fetch_file_for_work, initialiseLoggingForCommand
from imap_mag.config import AppSettings
from imap_mag.io import DatastoreFileFinder
from imap_mag.io.file import IALiRTPathHandler, IALiRTQuicklookPathHandler
from imap_mag.plot.plot_ialirt_files import plot_ialirt_files
from imap_mag.util import DatetimeProvider

logger = logging.getLogger(__name__)


# E.g.,
# imap-mag plot ialirt --start-date 2025-01-02 --end-date 2025-01-03
# imap-mag plot ialirt --files /path/to/file1 --files /path/to/file2
def plot_ialirt(
    start_date: Annotated[
        datetime | None, typer.Option(help="Start date of data to add to plot")
    ] = None,
    end_date: Annotated[
        datetime | None, typer.Option(help="End date of data to add to plot")
    ] = None,
    files: Annotated[
        list[Path] | None,
        typer.Option(
            help="List of files to add to plot",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            writable=False,
        ),
    ] = None,
) -> dict[Path, IALiRTQuicklookPathHandler]:
    """Plot I-ALiRT data."""

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.plot_ialirt)

    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    datastore_finder = DatastoreFileFinder(app_settings.data_store)

    if (
        (start_date is None)
        and (end_date is None)
        and (files is None or len(files) == 0)
    ):
        logger.info(
            "No start/end date or files provided, plotting yesterday's and today's data."
        )
        start_date = DatetimeProvider.yesterday()
        end_date = DatetimeProvider.today()

    if (start_date is not None) and (end_date is not None):
        logger.info(f"Plotting I-ALiRT data from {start_date} to {end_date}.")

        # Get unique range of dates
        unique_dates = pd.date_range(
            start=start_date, end=end_date, freq="d"
        ).to_pydatetime()

        path_handlers = [IALiRTPathHandler(content_date=date) for date in unique_dates]
        files = []

        for handler in path_handlers:
            f = datastore_finder.find_matching_file(handler, throw_if_not_found=False)
            if f is not None:
                files.append(f)

    if files is None or (len(files) == 0):
        logger.warning("No I-ALiRT files to plot.")
        return {}

    # Copy files to work folder
    work_files: list[Path] = []

    for f in files:
        work_files.append(fetch_file_for_work(f, work_folder, throw_if_not_found=True))

    logger.info(
        f"Plotting I-ALiRT data from {len(files)} files:\n{', '.join(f.as_posix() for f in work_files)}"
    )

    # Generate plots
    generated_figure: dict[Path, IALiRTQuicklookPathHandler] = plot_ialirt_files(
        work_files, save_folder=work_folder
    )

    ialirt_file_and_handler: dict[Path, IALiRTQuicklookPathHandler] = {}

    if app_settings.plot_ialirt.publish_to_data_store:
        output_manager = appUtils.getOutputManagerByMode(
            app_settings,
            use_database=False,
        )

        for file, path_handler in generated_figure.items():
            (output_file, output_handler) = output_manager.add_file(file, path_handler)
            ialirt_file_and_handler[output_file] = output_handler
    else:
        logger.info("Files not published to data store based on config.")
        ialirt_file_and_handler = generated_figure

    return ialirt_file_and_handler
