import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from imap_mag.cli.cliUtils import fetch_file_for_work
from imap_mag.io import DatastoreFileFinder
from imap_mag.io.file import IALiRTPathHandler
from imap_mag.util import DatetimeProvider

logger = logging.getLogger(__name__)


def fetch_ialirt_files_for_work(
    data_store: Path,
    work_folder: Path,
    start_date: datetime | None,
    end_date: datetime | None,
    files: list[Path] | None,
) -> list[Path]:
    datastore_finder = DatastoreFileFinder(data_store)

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
        return []

    # Copy files to work folder
    work_files: list[Path] = []

    for f in files:
        work_files.append(fetch_file_for_work(f, work_folder, throw_if_not_found=True))

    return work_files
