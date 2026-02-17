import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from imap_mag.cli.cliUtils import fetch_file_for_work
from imap_mag.io import DatastoreFileFinder
from imap_mag.io.file import IALiRTHKPathHandler, IALiRTPathHandler
from imap_mag.io.file.IFilePathHandler import IFilePathHandler
from imap_mag.util import DatetimeProvider

logger = logging.getLogger(__name__)


def fetch_ialirt_files_for_work(
    data_store: Path,
    work_folder: Path,
    start_date: datetime | None,
    end_date: datetime | None,
    files: list[Path] | None,
) -> list[Path]:
    """Fetch I-ALiRT MAG science files from the datastore."""

    return _fetch_files_for_work(
        data_store=data_store,
        work_folder=work_folder,
        start_date=start_date,
        end_date=end_date,
        files=files,
        path_handler_factory=lambda date: IALiRTPathHandler(content_date=date),
        label="I-ALiRT",
    )


def fetch_ialirt_hk_files_for_work(
    data_store: Path,
    work_folder: Path,
    start_date: datetime | None,
    end_date: datetime | None,
    files: list[Path] | None,
) -> list[Path]:
    """Fetch I-ALiRT MAG HK files from the datastore."""

    return _fetch_files_for_work(
        data_store=data_store,
        work_folder=work_folder,
        start_date=start_date,
        end_date=end_date,
        files=files,
        path_handler_factory=lambda date: IALiRTHKPathHandler(content_date=date),
        label="I-ALiRT HK",
    )


def _fetch_files_for_work(
    data_store: Path,
    work_folder: Path,
    start_date: datetime | None,
    end_date: datetime | None,
    files: list[Path] | None,
    path_handler_factory,
    label: str,
) -> list[Path]:
    datastore_finder = DatastoreFileFinder(data_store)

    if (
        (start_date is None)
        and (end_date is None)
        and (files is None or len(files) == 0)
    ):
        logger.info(
            "No start/end date or files provided, loading yesterday's and today's data."
        )
        start_date = DatetimeProvider.yesterday()
        end_date = DatetimeProvider.today()

    if (start_date is not None) and (end_date is not None):
        logger.info(f"Loading {label} data from {start_date} to {end_date}.")

        # Get unique range of dates
        unique_dates = pd.date_range(
            start=start_date, end=end_date, freq="d"
        ).to_pydatetime()

        path_handlers: list[IFilePathHandler] = [
            path_handler_factory(date) for date in unique_dates
        ]
        files = []

        for handler in path_handlers:
            f = datastore_finder.find_matching_file(handler, throw_if_not_found=False)
            if f is not None:
                files.append(f)

    if files is None or (len(files) == 0):
        logger.warning(f"No {label} files to load.")
        return []

    # Copy files to work folder
    work_files: list[Path] = []

    for f in files:
        work_files.append(fetch_file_for_work(f, work_folder, throw_if_not_found=True))

    return work_files
