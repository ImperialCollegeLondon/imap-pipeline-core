import collections
import logging
import re
from collections.abc import Generator
from copy import deepcopy
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import space_packet_parser as spp
import xarray as xr
from rich.progress import track
from space_packet_parser.exceptions import UnrecognizedPacketTypeError

from imap_mag.io import DatastoreFileFinder
from imap_mag.io.file import HKBinaryPathHandler, HKDecodedPathHandler, IFilePathHandler
from imap_mag.process.FileProcessor import FileProcessor
from imap_mag.process.get_packet_definition_folder import get_packet_definition_folder
from imap_mag.process.HKProcessSettings import HKProcessSettings
from imap_mag.util import (
    CONSTANTS,
    CCSDSBinaryPacketFile,
    HKPacket,
    Subsystem,
    TimeConversion,
)

logger = logging.getLogger(__name__)


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
        self.__xtcePacketDefinitionFolder = get_packet_definition_folder(
            packet_definition
        )

    def _packet_generator(
        self,
        packet_file: str | Path,
        xtce_packet_definition: str | Path,
    ) -> Generator[tuple[spp.SpacePacket, int], None, None]:
        """
        Parse packets from a packet file.

        Parameters
        ----------
        packet_file : str | Path
            Path to data packet path with filename.
        xtce_packet_definition : str | Path
            Path to XTCE file with filename.

        Yields
        ------
        packet : space_packet_parser.SpacePacket
            Parsed packet dictionary.
        """
        # Set up the parser from the input packet definition
        packet_definition = spp.load_xtce(xtce_packet_definition)

        with open(packet_file, "rb") as binary_data:
            for binary_packet in spp.ccsds_generator(binary_data):
                try:
                    packet = packet_definition.parse_bytes(binary_packet)
                except UnrecognizedPacketTypeError as e:
                    # NOTE: Not all of our definitions have all of the APIDs
                    #       we may encounter, so we only want to process ones
                    #       we can actually parse.
                    logger.warning(e)
                    continue
                yield packet, binary_packet.apid

    @staticmethod
    def _add_or_concat_dataframe(
        data_dict: dict[int, pd.DataFrame], key: int, value: pd.DataFrame
    ) -> pd.DataFrame:
        if key in data_dict:
            return pd.concat([data_dict[key], value])
        else:
            return value

    def process(
        self, files: Path | list[Path], raise_on_error: bool = False
    ) -> dict[Path, IFilePathHandler]:
        """Process HK with XTCE tools and create CSV file."""

        if isinstance(files, Path):
            files = [files]

        # Load input files.
        input_data: dict[int, pd.DataFrame] = self.__load_and_decommutate_files(
            files, raise_on_error=raise_on_error
        )

        # Load data for each ApID.
        datastore_data = self.__load_datastore_data(input_data)

        # The new (input) data is added last, such that it overrides existing data with same
        # APID-SHCOARSE-SEQCNT triplet, in case any new data is received.
        combined_data = datastore_data

        for apid, data in input_data.items():
            combined_data[apid] = self._add_or_concat_dataframe(
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

                # Add a new column for the date in ISO format
                daily_data["time_met_iso"] = pd.Series(
                    data=TimeConversion.convert_j2000ns_to_isostring(
                        daily_data.index.values
                    ),
                    index=daily_data.index,
                    dtype=str,
                )

                # Create an empty column for SPICE based time in human readable format - TBD
                daily_data["epoch_iso"] = pd.NA

                # Treat "epoch" as a variable, not an index
                daily_data.reset_index(inplace=True, names=CONSTANTS.CCSDS_FIELD.EPOCH)

                path, handler = self.__save_daily_data(
                    day,
                    daily_data,
                    path_handler,
                    HKProcessSettings.from_instrument(packet.instrument),
                )
                processed_files[path] = handler

        return processed_files

    def __load_datastore_data(
        self, input_data: dict[int, pd.DataFrame], raise_on_error: bool = False
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
                    day_files, raise_on_error=raise_on_error
                )
                assert (apid in day_data) and (len(day_data) == 1)

                datastore_data[apid] = self._add_or_concat_dataframe(
                    datastore_data, apid, day_data[apid]
                )

        return datastore_data

    def __load_and_decommutate_files(
        self, files: list[Path], raise_on_error: bool = False
    ) -> dict[int, pd.DataFrame]:
        """Load and decommutate packets from binary files."""

        dataframe_by_apid: dict[int, pd.DataFrame] = dict()

        for file in track(files, description="Decommutating HK files..."):
            try:
                results: dict[int, xr.Dataset] = self.__decommutate_packets(file)
            except Exception as e:
                logger.error(f"Failed to decommutate packets from {file}", exc_info=e)
                if raise_on_error:
                    raise e
                continue

            logger.info(
                f"Found {len(results.keys())} ApIDs ({', '.join(str(key) for key in results.keys())}) in {file}."
            )

            for apid, data in results.items():
                dataframe_by_apid[apid] = self._add_or_concat_dataframe(
                    dataframe_by_apid, apid, data.to_dataframe()
                )

        return dataframe_by_apid

    def __decommutate_packets(self, file: Path) -> dict[int, xr.Dataset]:
        """Decommutate packets from a binary file by using the XTCE definitions."""

        dataset_dict: dict[int, xr.Dataset] = {}

        # Extract data from binary file.
        data_dict: dict[int, dict] = dict()

        apids: set[int] = CCSDSBinaryPacketFile(file).get_apids()
        apids = self.__filter_unknown_apids(apids)

        subsystems: set[Subsystem] = {
            HKPacket.from_apid(apid).instrument for apid in apids
        }

        logger.info(
            f"Found {len(subsystems)} subsystems in {file!s}: {', '.join(s.name for s in subsystems)}"
        )

        for subsystem in subsystems:
            logger.debug(f"Processing subsystem: {subsystem.name}")

            packet_definition_path = (
                self.__xtcePacketDefinitionFolder / subsystem.tlm_db_file
            )

            if not packet_definition_path.exists():
                raise FileNotFoundError(
                    f"Packet definition file not found for subsystem {subsystem.name} at expected path: {packet_definition_path}"
                )

            for packet, apid in self._packet_generator(
                packet_file=file, xtce_packet_definition=packet_definition_path
            ):
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
        daily_data.to_csv(file_path, index=False)

        # Use a deep-copy, otherwise the same handle will be used for all files.
        return file_path, deepcopy(path_handler)
