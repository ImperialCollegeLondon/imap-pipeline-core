"""Program to retrieve and process NOAA RTSW mag and plasma data."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from imap_mag.client.NOAAApiClient import NOAARTSWApiClient
from imap_mag.io import FileFinder
from imap_mag.io.file import IFilePathHandler, NOAAPathHandler

logger = logging.getLogger(__name__)


class FetchNOAA:
    _DATE_INDEX = "time_tag"

    def __init__(
        self,
        data_access: NOAARTSWApiClient,
        work_folder: Path,
        datastore_finder: FileFinder,
    ):
        """Initialise interface."""

        self._data_access = data_access
        self._work_folder = work_folder
        self._datastore_finder = datastore_finder

    def _get_index_as_datetime(self, data: pd.DataFrame) -> pd.Series:
        """Transform the date index in a series of python datetime objects."""
        return pd.to_datetime(data[self._DATE_INDEX]).dt.to_pydatetime()

    def download_csv(
        self,
        spacecraft: Literal["SOLAR1", "ACE"],
        instrument: Literal["mag", "plasma"],
    ) -> dict[Path, IFilePathHandler]:
        """Downloads the data from the server and saves it as CSV.

        Args:
            spacecraft: The spacecraft to retrieve the data for. Must be "SOLAR1" or
                "ACE"
            instrument: The instrument to retrieve. Must be `mag` or `plasma`.

        Returns:
            A dicitonary of paths and path handlers with the data.
        """
        if spacecraft not in ("SOLAR1", "ACE"):
            raise ValueError(
                "Invalid spacecraft requested. "
                f"It must be 'SOLAR1' or 'ACE', but '{spacecraft}' found"
            )

        if instrument not in ("mag", "plasma"):
            raise ValueError(
                f"Invalid instrument type requested for {spacecraft}. "
                f"It must be 'mag' or 'plasma', but '{instrument}' found"
            )

        downloaded: list[dict[str, Any]] = self._data_access.get_data(
            spacecraft=spacecraft,
            instrument=instrument,
        )

        if not downloaded:
            logger.debug(
                f"No {spacecraft} {instrument} data downloaded from NOAA RTSW."
            )
            return dict()

        process_fn = _process_noaa_mag if instrument == "mag" else _process_noaa_plasma
        downloaded_data = process_fn(pd.DataFrame(downloaded))
        return self._add_to_files(spacecraft, instrument, downloaded_data)

    def _add_to_files(
        self,
        spacecraft: Literal["SOLAR1", "ACE"],
        instrument: Literal["mag", "plasma"],
        data: pd.DataFrame,
    ) -> dict[Path, IFilePathHandler]:
        """Add downloaded data to existing (or new) files."""
        downloaded_files: dict[Path, IFilePathHandler] = dict()

        dates = pd.to_datetime(data[self._DATE_INDEX]).dt.date
        unique_dates = dates.unique()

        logger.info(
            f"Downloaded {spacecraft} {instrument} for {len(unique_dates)} "
            f"days: {', '.join(d.strftime('%Y-%m-%d') for d in unique_dates)}"
        )

        for day_info, daily_data in data.groupby(dates):
            date: datetime = day_info[0] if isinstance(day_info, tuple) else day_info  # type: ignore

            daily_dates = self._get_index_as_datetime(daily_data)
            min_daily_date = min(daily_dates)
            max_daily_date = max(daily_dates)

            path_handler = NOAAPathHandler(
                mission=spacecraft, instrument=instrument, content_date=max_daily_date
            )

            # Find file in datastore
            file_path: Path | None = self._datastore_finder.find_by_handler(
                path_handler, throw_if_not_found=False
            )

            if file_path is not None and file_path.exists():
                logger.debug(
                    f"File for {date.strftime('%Y-%m-%d')} already exists: "
                    f"{file_path.as_posix()}. Appending new data."
                )
                existing_data = pd.read_csv(file_path)
            else:
                logger.debug(f"Creating new file for {date.strftime('%Y-%m-%d')}.")

                file_path = self._work_folder / path_handler.get_filename()
                existing_data = pd.DataFrame()

            # Add data to file
            # If data is completely new, just append new data.
            # We still need to load the existing data, as some columns may be missing in the new data.
            # If the data already in the data store has fewer columns than the new data, we need to rewrite the file.
            combined_data = pd.concat([existing_data, daily_data])

            if (
                not existing_data.empty
                and (len(existing_data.columns) >= len(daily_data.columns))
                and (max(self._get_index_as_datetime(existing_data)) < min_daily_date)
            ):
                # Only append the new data.
                combined_data = combined_data[
                    self._get_index_as_datetime(combined_data).values >= min_daily_date
                ]
                write_mode = "a"
            else:
                write_mode = "w"

            # Sort data by time and remove any duplicates (by keeping the latest entries)
            # Use DATE_INDEX as index and reorder the columns alphabetically
            combined_data.drop_duplicates(
                subset=self._DATE_INDEX, keep="last", inplace=True
            )
            combined_data.sort_values(by=self._DATE_INDEX, inplace=True)
            combined_data.dropna(axis="index", subset=[self._DATE_INDEX], inplace=True)
            combined_data.set_index(self._DATE_INDEX, inplace=True, drop=True)
            combined_data = combined_data.reindex(
                sorted(combined_data.columns), axis="columns"
            )

            combined_data.to_csv(
                file_path, mode=write_mode, header=(write_mode == "w"), index=True
            )
            logger.debug(
                f"{spacecraft} {instrument} data {'written' if write_mode == 'w' else 'appended'} to {file_path.as_posix()}."
            )

            downloaded_files[file_path] = path_handler

        return downloaded_files


def _process_noaa_mag(data: pd.DataFrame) -> pd.DataFrame:
    """Process the mag data to pick only the relevant columns.

    Args:
        data: Mag data to process.

    Returns:
        Dataframe with processed mag data.
    """
    expected_columns = [
        "time_tag",
        "bx_gsm",
        "by_gsm",
        "bz_gsm",
        "theta_gsm",
        "phi_gsm",
    ]
    return data[expected_columns]


def _process_noaa_plasma(data: pd.DataFrame) -> pd.DataFrame:
    """Process the plasma data to pick only the relevant columns.

    It also renames the columns to remove the 'proton_' prefix.

    Args:
        data: Plasma data to process.

    Returns:
        Dataframe with processed plasma data.
    """
    expected_columns = [
        "time_tag",
        "proton_speed",
        "proton_temperature",
        "proton_density",
    ]
    data: pd.DataFrame = data[expected_columns]
    return data.rename(
        columns={
            "proton_speed": "speed",
            "proton_temperature": "temperature",
            "proton_density": "density",
        }
    )
