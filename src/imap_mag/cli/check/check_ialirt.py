import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.check import IALiRTAnomaly, check_ialirt_files
from imap_mag.cli.cliUtils import initialiseLoggingForCommand
from imap_mag.cli.ialirtUtils import fetch_ialirt_files_for_work
from imap_mag.config import AppSettings

logger = logging.getLogger(__name__)


class IALiRTAnomalyError(Exception):
    """Custom exception for I-ALiRT anomaly errors."""

    pass


# E.g.,
# imap-mag check ialirt --start-date 2025-01-02 --end-date 2025-01-03
# imap-mag check ialirt --files /path/to/file1 --files /path/to/file2
def check_ialirt(
    start_date: Annotated[
        datetime | None, typer.Option(help="Start date of data to check")
    ] = None,
    end_date: Annotated[
        datetime | None, typer.Option(help="End date of data to check")
    ] = None,
    files: Annotated[
        list[Path] | None,
        typer.Option(
            help="List of files to check",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            writable=False,
        ),
    ] = None,
    error_on_failure: Annotated[
        bool,
        typer.Option(
            "--error-on-failure",
            help="Error if any data contains anomalies",
            show_default=True,
        ),
    ] = True,
) -> list[IALiRTAnomaly]:
    """Check I-ALiRT data for anomalies."""

    app_settings = AppSettings()  # type: ignore
    work_folder = app_settings.setup_work_folder_for_command(app_settings.check_ialirt)

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
        return []

    logger.info(
        f"Checking I-ALiRT data from {len(work_files)} files:\n{', '.join(f.as_posix() for f in work_files)}"
    )

    anomalies: list[IALiRTAnomaly] = check_ialirt_files(
        work_files,
        app_settings.packet_definition,
    )

    if anomalies:
        logger.error(f"Detected {len(anomalies)} anomalies in I-ALiRT data:")

        for anomaly in anomalies:
            anomaly.log()

        if error_on_failure:
            raise IALiRTAnomalyError("I-ALiRT data contains anomalies.")
    else:
        logger.info("No anomalies detected in I-ALiRT data.")

    return anomalies
