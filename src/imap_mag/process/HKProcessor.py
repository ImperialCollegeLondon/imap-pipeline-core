import collections
import logging
import os
import re
from copy import deepcopy
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import xarray as xr
from rich.progress import track
from space_packet_parser import definitions

from imap_mag.io import DatastoreFileFinder, HKPathHandler, IFilePathHandler
from imap_mag.process.FileProcessor import FileProcessor
from imap_mag.util import (
    CONSTANTS,
    CCSDSBinaryPacketFile,
    HKLevel,
    HKPacket,
    TimeConversion,
)

logger = logging.getLogger(__name__)


def add_or_concat_dataframe(
    data_dict: dict[int, pd.DataFrame], key: int, value: pd.DataFrame
) -> pd.DataFrame:
    if key in data_dict:
        return pd.concat([data_dict[key], value])
    else:
        return value


class HKProcessor(FileProcessor):
    xtcePacketDefinition: Path

    def __init__(
        self, work_folder: Path, datastore_finder: DatastoreFileFinder
    ) -> None:
        self.__work_folder = work_folder
        self.__datastore_finder = datastore_finder

    def is_supported(self, file: Path) -> bool:
        return file.suffix in [".pkts", ".bin"]

    def initialize(self, packet_definition: Path) -> None:
        paths_to_try: dict[str, Path] = {
            "relative": packet_definition,
            "module": Path(os.path.dirname(__file__)).parent / packet_definition,
            "default": Path("tlm.xml"),
        }

        paths_to_try_string: str = "\n".join(
            [f"    {source}: {path}" for source, path in paths_to_try.items()]
        )
        logger.debug(
            f"Trying XTCE packet definition file from these paths in turn:\n{paths_to_try_string}"
        )

        for source, path in paths_to_try.items():
            if path and path.exists():
                logger.debug(
                    f"Using XTCE packet definition file from {source} path: {path}"
                )
                self.xtcePacketDefinition = path
                break
        else:
            raise FileNotFoundError(
                f"XTCE packet definition file not found: {packet_definition}"
            )

    def process(self, files: Path | list[Path]) -> dict[Path, IFilePathHandler]:
        """Process HK with XTCE tools and create CSV file."""

        if isinstance(files, Path):
            files = [files]

        # Load binary files and extract dates.
        days_by_apid: dict[int, set[date]] = CCSDSBinaryPacketFile.combine_days_by_apid(
            files
        )
        logger.info(
            f"Found {len(days_by_apid)} ApIDs in {len(files)} files: {', '.join(str(apid) for apid in sorted(days_by_apid.keys()))}"
        )

        # Filter out non-MAG ApIDs.
        days_by_apid = self.__filter_mag_apids(days_by_apid)

        # Load data for each ApID.
        datastore_data, datastore_files = self.__load_datastore_data(days_by_apid)

        # If original files are not in the datastore, load them.
        # This data is loaded last, such that it overrides existing data with same APID-SHCOARSE-SEQCNT
        # triplet, in case any new data is received.
        datastore_data: dict[int, pd.DataFrame] = self.__load_input_files(
            files, datastore_files, datastore_data
        )

        # Split each ApID into a separate file per day.
        processed_files: dict[Path, IFilePathHandler] = {}

        for apid, data in datastore_data.items():
            hk_packet: str = HKPacket.from_apid(apid).packet
            path_handler = HKPathHandler(
                level=HKLevel.l1.value,
                descriptor=HKPathHandler.convert_packet_to_descriptor(hk_packet),
                content_date=None,
                extension="csv",
            )

            # Split data by day.
            dates: list[date] = TimeConversion.convert_j2000ns_to_date(
                data.index.values
            )
            logger.info(
                f"Splitting data for ApID {apid} ({hk_packet}) into separate files for each day:\n"
                f"{', '.join(d.strftime('%Y%m%d') for d in sorted(set(dates)))}"
            )

            for day_info, daily_data in data.groupby(dates):
                day: date = day_info[0] if isinstance(day_info, tuple) else day_info  # type: ignore

                path, handler = self.__save_daily_data(day, daily_data, path_handler)
                processed_files[path] = handler

        return processed_files

    def __filter_mag_apids(
        self, days_by_apid: dict[int, set[date]]
    ) -> dict[int, set[date]]:
        """Filter out non-MAG ApIDs."""

        non_mag_apids: list[int] = [
            apid
            for apid in days_by_apid.keys()
            if apid
            not in range(CONSTANTS.MAG_APID_RANGE[0], CONSTANTS.MAG_APID_RANGE[1] + 1)
        ]

        if non_mag_apids:
            logger.warning(
                f"Filtering out non-MAG ApIDs: {', '.join(str(apid) for apid in sorted(non_mag_apids))}"
            )

            days_by_apid = {
                apid: days
                for apid, days in days_by_apid.items()
                if apid not in non_mag_apids
            }

        return days_by_apid

    def __load_datastore_data(
        self, days_by_apid: dict[int, set[date]]
    ) -> tuple[dict[int, pd.DataFrame], set[Path]]:
        """Load existing data from the datastore for each ApID and day."""

        datastore_data: dict[int, pd.DataFrame] = dict()
        datastore_files: set[Path] = set()

        for apid, days in days_by_apid.items():
            hk_packet: str = HKPacket.from_apid(apid).packet
            logger.info(
                f"Processing ApID {apid} ({hk_packet}) for days:\n{', '.join(d.strftime('%Y-%m-%d') for d in sorted(days))}"
            )
            datastore_data.setdefault(apid, pd.DataFrame())

            for day in days:
                l0_path_handler = HKPathHandler(
                    level=HKLevel.l0.value,
                    descriptor=HKPathHandler.convert_packet_to_descriptor(hk_packet),
                    content_date=datetime.combine(day, datetime.min.time()),
                    extension="pkts",
                )

                day_files: list[Path] = self.__datastore_finder.find_all_file_sequences(
                    l0_path_handler, throw_if_not_found=False
                )

                if not day_files:
                    logger.debug(
                        f"No existing files found for {hk_packet} on {day.strftime('%Y-%m-%d')} in datastore."
                    )
                    continue
                else:
                    logger.info(
                        f"Found {len(day_files)} existing files for {hk_packet} on {day.strftime('%Y-%m-%d')} in datastore."
                    )

                day_data: dict[int, pd.DataFrame] = self.__load_and_decommutate_files(
                    day_files
                )
                assert (apid in day_data) and (len(day_data) == 1)

                datastore_data[apid] = add_or_concat_dataframe(
                    datastore_data, apid, day_data[apid]
                )
                datastore_files.update(day_files)

        return datastore_data, datastore_files

    def __load_input_files(
        self,
        files: list[Path],
        datastore_files: set[Path],
        datastore_data: dict[int, pd.DataFrame],
    ) -> dict[int, pd.DataFrame]:
        """Load files that are not in the datastore"""

        new_files: list[Path] = [
            file for file in files if file.absolute().as_posix() not in datastore_files
        ]
        logger.info(
            f"Loading {len(new_files)} new files that are not in the datastore:\n{', '.join(str(file) for file in new_files)}"
        )

        new_data: dict[int, pd.DataFrame] = self.__load_and_decommutate_files(new_files)

        for apid, df in new_data.items():
            datastore_data[apid] = add_or_concat_dataframe(datastore_data, apid, df)

        return datastore_data

    def __load_and_decommutate_files(
        self, files: list[Path]
    ) -> dict[int, pd.DataFrame]:
        """Load and decommutate packets from binary files."""

        dataframe_by_apid: dict[int, pd.DataFrame] = dict()

        for file in track(files, description="Processing HK files..."):
            results: dict[int, xr.DataArray] = self.__decommutate_packets(file)
            logger.info(
                f"Found {len(results.keys())} ApIDs ({', '.join(str(key) for key in results.keys())}) in {file}."
            )

            for apid, data in results.items():
                dataframe_by_apid[apid] = add_or_concat_dataframe(
                    dataframe_by_apid, apid, data.to_dataframe()
                )

        return dataframe_by_apid

    def __decommutate_packets(self, file: Path) -> dict[int, xr.DataArray]:
        """Decommutate packets from a binary file by using the XTCE definitions."""

        # Extract data from binary file.
        data_dict: dict[int, dict] = dict()

        packet_definition = definitions.XtcePacketDefinition(self.xtcePacketDefinition)

        with open(file, "rb") as binary_data:
            packet_generator = packet_definition.packet_generator(
                binary_data,
                show_progress=False,  # Prefect will log this every 0.001 seconds, which is insane
            )

            for packet in packet_generator:
                apid = packet.header["PKT_APID"].raw_value
                data_dict.setdefault(apid, collections.defaultdict(list))

                packet_content = packet.user_data | packet.header

                for key, value in packet_content.items():
                    if value is None:
                        value = value.raw_value
                    elif hasattr(value, "decode"):
                        value = int.from_bytes(value, byteorder="big")

                    updated_key = re.sub(r"^mag_\w+?\.", "", key.lower())
                    data_dict[apid][updated_key].append(value)

        # Convert data to xarray datasets.
        dataset_dict: dict[int, xr.Dataset] = {}

        for apid, data in data_dict.items():
            time_key = next(iter(data.keys()))
            time_data = TimeConversion.convert_met_to_j2000ns(data[time_key])

            ds = xr.Dataset(
                {key: ("epoch", value) for key, value in data.items()},
                coords={"epoch": time_data},
            )
            ds = ds.sortby("epoch")

            dataset_dict[apid] = ds

        return dataset_dict

    def __save_daily_data(
        self, day: date, daily_data: pd.DataFrame, path_handler: HKPathHandler
    ) -> tuple[Path, HKPathHandler]:
        """Save data by day to a CSV file in the work folder."""

        logger.debug(f"Generating file for {day.strftime('%Y-%m-%d')}.")

        path_handler.content_date = datetime.combine(day, datetime.min.time())
        file_path = self.__work_folder / path_handler.get_filename()

        # Save to CSV.
        daily_data.drop_duplicates(
            subset=[
                CONSTANTS.CCSDS_FIELD.APID,
                CONSTANTS.CCSDS_FIELD.SHCOARSE,
                CONSTANTS.CCSDS_FIELD.SEQ_COUNTER,
            ],
            keep="last",
            inplace=True,
        )
        daily_data.sort_values(
            by=[CONSTANTS.CCSDS_FIELD.SHCOARSE, CONSTANTS.CCSDS_FIELD.SEQ_COUNTER],
            inplace=True,
        )
        daily_data.to_csv(file_path)

        # Use a deep-copy, otherwise the same handle will be used for all files.
        return file_path, deepcopy(path_handler)
