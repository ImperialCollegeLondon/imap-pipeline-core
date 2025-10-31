import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag import appUtils
from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.cli.ialirtUtils import fetch_ialirt_files_for_work
from imap_mag.config import AppSettings, SaveMode
from imap_mag.io.file import IALiRTQuicklookPathHandler
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
    combined_plot: Annotated[
        bool,
        typer.Option(
            "--combined",
            help="Whether to combine all I-ALiRT data into a single figure",
        ),
    ] = False,
    save_mode: Annotated[
        SaveMode,
        typer.Option(help="Whether to save locally only or to also save to database"),
    ] = SaveMode.LocalOnly,
) -> dict[Path, IALiRTQuicklookPathHandler]:
    """Plot I-ALiRT data."""

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.plot_ialirt)

    initialiseLoggingForCommand(
        work_folder
    )  # DO NOT log anything before this point (it won't be captured in the log file)

    work_files = fetch_ialirt_files_for_work(
        app_settings.data_store,
        work_folder,
        start_date=start_date,
        end_date=end_date,
        files=files,
    )

    if len(work_files) == 0:
        return {}

    logger.info(
        f"Plotting I-ALiRT data from {len(work_files)} files:\n{', '.join(f.as_posix() for f in work_files)}"
    )

    # Generate plots
    generated_figure: dict[Path, IALiRTQuicklookPathHandler] = plot_ialirt_files(
        work_files, save_folder=work_folder, combine_plots=combined_plot
    )

    ialirt_file_and_handler: dict[Path, IALiRTQuicklookPathHandler] = {}

    if app_settings.plot_ialirt.publish_to_data_store:
        output_manager = appUtils.getOutputManagerByMode(
            app_settings,
            use_database=(save_mode == SaveMode.LocalAndDatabase),
        )

        for file, path_handler in generated_figure.items():
            (output_file, output_handler) = output_manager.add_file(file, path_handler)
            ialirt_file_and_handler[output_file] = output_handler

            # Add "latest" copy for today
            if path_handler.content_date and (
                path_handler.content_date.replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                == DatetimeProvider.today()
            ):
                shutil.copy(
                    output_file,
                    app_settings.data_store
                    / output_handler.root_folder
                    / "ialirt"
                    / "latest.png",
                )
    else:
        logger.info("Files not published to data store based on config.")
        ialirt_file_and_handler = generated_figure

    return ialirt_file_and_handler
