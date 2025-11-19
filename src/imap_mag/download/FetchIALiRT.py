"""Program to retrieve and process MAG CDF files."""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml

from imap_mag.client.IALiRTApiClient import IALiRTApiClient
from imap_mag.io import DatastoreFileFinder
from imap_mag.io.file import IALiRTPathHandler
from imap_mag.process import get_packet_definition_folder
from imap_mag.util.constants import CONSTANTS

logger = logging.getLogger(__name__)


class FetchIALiRT:
    """Manage I-ALiRT data."""

    __DATE_INDEX = "met_in_utc"
    __IALIRT_PACKET_DEFINITION_FILE = CONSTANTS.IALIRT_PACKET_DEFINITION_FILE

    def __init__(
        self,
        data_access: IALiRTApiClient,
        work_folder: Path,
        datastore_finder: DatastoreFileFinder,
        packet_definition: Path,
    ) -> None:
        """Initialize I-ALiRT interface."""

        self.__data_access = data_access
        self.__work_folder = work_folder
        self.__datastore_finder = datastore_finder
        self.__packetDefinitionFolder = get_packet_definition_folder(packet_definition)

    def download_ialirt_to_csv(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> dict[Path, IALiRTPathHandler]:
        """Retrieve I-ALiRT data."""

        downloaded_files: dict[Path, IALiRTPathHandler] = dict()

        downloaded: list[dict] = self.__data_access.get_all_by_dates(
            start_date=start_date, end_date=end_date
        )

        if downloaded:
            logger.info(
                f"Downloaded {len(downloaded)} entries from I-ALiRT Data Access."
            )

            downloaded_data = pd.DataFrame(downloaded)
            downloaded_data = process_ialirt_data(
                downloaded_data,
                self.__packetDefinitionFolder / self.__IALIRT_PACKET_DEFINITION_FILE,
            )

            # Aggregate data by multiple instruments per timestamp
            rules: dict = dict.fromkeys(downloaded_data, "first")
            del rules[self.__DATE_INDEX]

            if "instrument" in rules:
                rules["instrument"] = lambda x: ",".join(x.dropna().unique())

            downloaded_data = (
                downloaded_data.groupby(self.__DATE_INDEX)
                .aggregate(rules)
                .reset_index()
            )

            downloaded_dates = pd.to_datetime(
                downloaded_data[self.__DATE_INDEX]
            ).dt.date
            unique_dates = downloaded_dates.unique()

            logger.info(
                f"Downloaded I-ALiRT data for {len(unique_dates)} days: {', '.join(d.strftime('%Y-%m-%d') for d in unique_dates)}"
            )

            for day_info, daily_data in downloaded_data.groupby(downloaded_dates):
                date: datetime = (
                    day_info[0] if isinstance(day_info, tuple) else day_info
                )  # type: ignore

                daily_dates = self.__get_index_as_datetime(daily_data)
                min_daily_date = min(daily_dates)
                max_daily_date = max(daily_dates)

                path_handler = IALiRTPathHandler(content_date=max_daily_date)

                # Find file in datastore
                file_path: Path | None = self.__datastore_finder.find_matching_file(
                    path_handler, throw_if_not_found=False
                )

                if file_path is not None and file_path.exists():
                    logger.debug(
                        f"File for {date.strftime('%Y-%m-%d')} already exists: {file_path.as_posix()}. Appending new data."
                    )
                    existing_data = pd.read_csv(file_path)
                else:
                    logger.debug(f"Creating new file for {date.strftime('%Y-%m-%d')}.")

                    file_path = self.__work_folder / path_handler.get_filename()
                    existing_data = pd.DataFrame()

                # Add data to file
                # If data is completely new, just append new data.
                # We still need to load the existing data, as some columns may be missing in the new data.
                # If the data already in the data store has fewer columns than the new data, we need to rewrite the file.
                combined_data = pd.concat([existing_data, daily_data])

                if (
                    not existing_data.empty
                    and (len(existing_data.columns) >= len(daily_data.columns))
                    and (
                        max(self.__get_index_as_datetime(existing_data))
                        < min_daily_date
                    )
                ):
                    # Only append the new data.
                    combined_data = combined_data[
                        self.__get_index_as_datetime(combined_data) >= min_daily_date
                    ]
                    write_mode = "a"
                else:
                    write_mode = "w"

                # Sort data by MET and remove any duplicates (by keeping the latest entries)
                # Use MET as index and reorder the columns alphabetically
                combined_data.drop_duplicates(
                    subset=self.__DATE_INDEX, keep="last", inplace=True
                )
                combined_data.sort_values(by=self.__DATE_INDEX, inplace=True)
                combined_data.dropna(
                    axis="index", subset=[self.__DATE_INDEX], inplace=True
                )
                combined_data.set_index(self.__DATE_INDEX, inplace=True, drop=True)
                combined_data = combined_data.reindex(
                    sorted(combined_data.columns), axis="columns"
                )

                combined_data.to_csv(
                    file_path, mode=write_mode, header=(write_mode == "w"), index=True
                )
                logger.debug(
                    f"I-ALiRT data {'written' if write_mode == 'w' else 'appended'} to {file_path.as_posix()}."
                )

                downloaded_files[file_path] = path_handler
        else:
            logger.debug("No data downloaded from I-ALiRT Data Access.")

        return downloaded_files

    def __get_index_as_datetime(self, data: pd.DataFrame):
        return pd.to_datetime(data[self.__DATE_INDEX]).dt.to_pydatetime()


def process_ialirt_data(df: pd.DataFrame, packet_definition_file: Path) -> pd.DataFrame:
    """Process I-ALiRT file to expand list columns."""

    df.columns = df.columns.str.strip()

    # Find columns that contain 3-element lists
    is_3element_list = lambda x: isinstance(x, list) and len(x) == 3  # noqa: E731

    columns_to_split = []
    for column in df.columns:
        if df[df[column].notna()][column].apply(is_3element_list).all():
            columns_to_split.append(column)

    for column in columns_to_split:
        # Parse the string representation of lists
        column_data = df[df[column].notna()][column]

        # Determine which suffixes to use based on the original column name
        if column.lower().endswith("_gse") or column.lower().endswith("_gsm"):
            suffixes = ["x", "y", "z"]
        elif column.lower().endswith("_rtn"):
            suffixes = ["r", "t", "n"]
        else:
            suffixes = ["1", "2", "3"]

        # Create new columns
        df[f"{column}_{suffixes[0]}"] = column_data.apply(lambda x: x[0])
        df[f"{column}_{suffixes[1]}"] = column_data.apply(lambda x: x[1])
        df[f"{column}_{suffixes[2]}"] = column_data.apply(lambda x: x[2])

        df = df.drop(columns=[column])

    # Extract MAG HK
    packet_definition: dict = yaml.safe_load(packet_definition_file.read_text())
    ialirt_packet_definition: dict = {
        col["name"]: col
        for col in packet_definition["ialirt_csv_conversion"]["columns"]
    }

    if "mag_hk_status" in df.columns:
        # drop rows without the status
        df_notna = df[df["mag_hk_status"].notna()]
        column_hk = df_notna["mag_hk_status"]

        # Convert to DataFrame and add prefix to column names
        dict_df = pd.DataFrame(column_hk.tolist(), index=df_notna.index)
        dict_df.columns = [f"mag_hk_{field}" for field in dict_df.columns]

        # Convert from engineering units
        for col, conversion in ialirt_packet_definition.items():
            if col in dict_df.columns:
                match conversion["type"]:
                    case "polynomial":
                        coeffs = conversion["coefficients"]

                        def polynomial_conversion(x):
                            return sum(c * (x**i) for i, c in enumerate(coeffs))

                        dict_df[col] = dict_df[col].apply(polynomial_conversion)
                    case "mapping":
                        mapping = conversion["lookup"]

                        def mapping_conversion(x):
                            return mapping[x]

                        dict_df[col] = dict_df[col].apply(mapping_conversion)
                    case _:
                        raise ValueError(
                            f"Unknown conversion type '{conversion['type']}' for column '{col}'."
                        )

        # Drop original column and concatenate new columns
        df = df.drop(columns=["mag_hk_status"])
        df = pd.concat([df, dict_df], axis="columns")

    return df
