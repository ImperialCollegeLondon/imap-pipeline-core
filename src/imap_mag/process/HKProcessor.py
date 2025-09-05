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

from imap_mag.io import DatastoreFileFinder
from imap_mag.io.file import HKBinaryPathHandler, HKDecodedPathHandler, IFilePathHandler
from imap_mag.process.FileProcessor import FileProcessor
from imap_mag.process.HKProcessSettings import HKProcessSettings
from imap_mag.util import (
    CCSDSBinaryPacketFile,
    HKPacket,
    Subsystem,
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
    __xtcePacketDefinitionFolder: Path

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
        }

        paths_to_try_string: str = "\n".join(
            [f"    {source}: {path}" for source, path in paths_to_try.items()]
        )
        logger.debug(
            f"Trying XTCE packet definition folder from these paths in turn:\n{paths_to_try_string}"
        )

        for source, path in paths_to_try.items():
            if path and path.exists():
                logger.debug(
                    f"Using XTCE packet definition folder from {source} path: {path}"
                )
                self.__xtcePacketDefinitionFolder = path
                break
        else:
            raise FileNotFoundError(
                f"XTCE packet definition folder not found: {packet_definition}"
            )

    def process(self, files: Path | list[Path]) -> dict[Path, IFilePathHandler]:
        """Process HK with XTCE tools and create CSV file."""

        if isinstance(files, Path):
            files = [files]

        # Load input files.
        input_data: dict[int, pd.DataFrame] = self.__load_and_decommutate_files(files)

        # Load data for each ApID.
        datastore_data = self.__load_datastore_data(input_data)

        # The new (input) data is added last, such that it overrides existing data with same
        # APID-SHCOARSE-SEQCNT triplet, in case any new data is received.
        combined_data = datastore_data

        for apid, data in input_data.items():
            combined_data[apid] = add_or_concat_dataframe(
                combined_data, apid, input_data[apid]
            )

        # Split each ApID into a separate file per day.
        processed_files: dict[Path, IFilePathHandler] = {}

        for apid, data in combined_data.items():
            packet: HKPacket = HKPacket.from_apid(apid)
            packet_name: str = packet.packet_name

            path_handler = HKDecodedPathHandler(
                instrument=packet.instrument.short_name,
                descriptor=HKDecodedPathHandler.convert_packet_to_descriptor(
                    packet_name
                ),
                content_date=None,
                extension="csv",
            )

            # Split data by day.
            dates: list[date] = TimeConversion.convert_j2000ns_to_date(
                data.index.values
            )
            logger.info(
                f"Splitting data for ApID {apid} ({packet_name}) into separate files for each day:\n"
                f"{', '.join(d.strftime('%Y%m%d') for d in sorted(set(dates)))}"
            )

            for day_info, daily_data in data.groupby(dates):
                day: date = day_info[0] if isinstance(day_info, tuple) else day_info  # type: ignore

                path, handler = self.__save_daily_data(
                    day,
                    daily_data,
                    path_handler,
                    HKProcessSettings.from_instrument(packet.instrument),
                )
                processed_files[path] = handler

        return processed_files

    def __load_datastore_data(
        self, input_data: dict[int, pd.DataFrame]
    ) -> dict[int, pd.DataFrame]:
        """Load existing data from the datastore for each ApID and day."""

        datastore_data: dict[int, pd.DataFrame] = dict()
        days_by_apid: dict[int, set[date]] = {}

        for apid, data in input_data.items():
            days_by_apid.setdefault(apid, set()).update(
                TimeConversion.convert_j2000ns_to_date(data.index.values)
            )

        for apid, days in days_by_apid.items():
            packet: HKPacket = HKPacket.from_apid(apid)
            packet_name: str = packet.packet_name

            logger.info(
                f"Processing ApID {apid} ({packet_name}) for days:\n{', '.join(d.strftime('%Y-%m-%d') for d in sorted(days))}"
            )
            datastore_data.setdefault(apid, pd.DataFrame())

            for day in days:
                l0_path_handler = HKBinaryPathHandler(
                    instrument=packet.instrument.short_name,
                    descriptor=HKBinaryPathHandler.convert_packet_to_descriptor(
                        packet_name
                    ),
                    content_date=datetime.combine(day, datetime.min.time()),
                    extension="pkts",
                )

                day_files: list[Path] = self.__datastore_finder.find_all_file_parts(
                    l0_path_handler, throw_if_not_found=False
                )

                if not day_files:
                    logger.debug(
                        f"No existing files found for {packet_name} on {day.strftime('%Y-%m-%d')} in datastore."
                    )
                    continue
                else:
                    logger.info(
                        f"Found {len(day_files)} existing files for {packet_name} on {day.strftime('%Y-%m-%d')} in datastore."
                    )

                day_data: dict[int, pd.DataFrame] = self.__load_and_decommutate_files(
                    day_files
                )
                assert (apid in day_data) and (len(day_data) == 1)

                datastore_data[apid] = add_or_concat_dataframe(
                    datastore_data, apid, day_data[apid]
                )

        return datastore_data

    def __load_and_decommutate_files(
        self, files: list[Path]
    ) -> dict[int, pd.DataFrame]:
        """Load and decommutate packets from binary files."""

        dataframe_by_apid: dict[int, pd.DataFrame] = dict()

        for file in track(files, description="Decommutating HK files..."):
            try:
                results: dict[int, xr.Dataset] = self.__decommutate_packets(file)
            except Exception as e:
                logger.error(f"Failed to decommutate packets from {file}:\n{e}")
                continue

            logger.info(
                f"Found {len(results.keys())} ApIDs ({', '.join(str(key) for key in results.keys())}) in {file}."
            )

            for apid, data in results.items():
                dataframe_by_apid[apid] = add_or_concat_dataframe(
                    dataframe_by_apid, apid, data.to_dataframe()
                )

        return dataframe_by_apid

    def __decommutate_packets(self, file: Path) -> dict[int, xr.Dataset]:
        """Decommutate packets from a binary file by using the XTCE definitions."""

        # Extract data from binary file.
        data_dict: dict[int, dict] = dict()

        apids: set[int] = CCSDSBinaryPacketFile(file).get_apids()
        apids = self.__filter_unknown_apids(apids)

        instruments: set[Subsystem] = {
            HKPacket.from_apid(apid).instrument for apid in apids
        }

        if not instruments:
            logger.debug(f"No valid data found in {file!s}.")
            return {}
        elif len(instruments) > 1:
            msg = f"File {file} contains ApIDs for {', '.join([i.name for i in instruments])}. Binary files with hybrid instrument data are not supported."

            logger.error(msg)
            raise RuntimeError(msg)
        else:
            instrument = instruments.pop()
            logger.debug(
                f"ApIDs {', '.join([str(apid) for apid in apids])} belong to {instrument.name} instrument."
            )

        packet_definition = definitions.XtcePacketDefinition(
            self.__xtcePacketDefinitionFolder
            / f"{instrument.value}_{instrument.tlm_db_version}.xml"
        )

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

                    match_packet_name_prefix_regex = r"^\w+?_\w+?\."
                    packet_field_name = re.sub(
                        match_packet_name_prefix_regex, "", key.lower()
                    )

                    data_dict[apid][packet_field_name].append(value)

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

    def __filter_unknown_apids(self, apids: set[int]) -> set[int]:
        """Filter out unknown ApIDs."""

        non_mag_apids: set[int] = {
            apid for apid in apids if apid not in HKPacket.apids()
        }

        if non_mag_apids:
            logger.warning(
                f"Unrecognized ApIDs will be ignored: {', '.join(str(apid) for apid in sorted(non_mag_apids))}"
            )

            apids = apids - non_mag_apids

        return apids

    def __save_daily_data(
        self,
        day: date,
        daily_data: pd.DataFrame,
        path_handler: HKDecodedPathHandler,
        process_settings: HKProcessSettings,
    ) -> tuple[Path, HKDecodedPathHandler]:
        """Save data by day to a CSV file in the work folder."""

        logger.debug(f"Generating file for {day.strftime('%Y-%m-%d')}.")

        path_handler.content_date = datetime.combine(day, datetime.min.time())
        file_path = self.__work_folder / path_handler.get_filename()

        # Save to CSV.
        daily_data.drop_duplicates(
            subset=process_settings.drop_duplicate_variables,
            keep="last",
            inplace=True,
        )
        daily_data.sort_values(
            by=process_settings.sort_variables,
            inplace=True,
        )
        daily_data.to_csv(file_path)

        # Use a deep-copy, otherwise the same handle will be used for all files.
        return file_path, deepcopy(path_handler)
