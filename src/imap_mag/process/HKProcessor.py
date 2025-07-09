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

from imap_mag.io import HKPathHandler, IFilePathHandler, InputManager
from imap_mag.process.FileProcessor import FileProcessor
from imap_mag.util import HKLevel, HKPacket, TimeConversion

logger = logging.getLogger(__name__)


class HKProcessor(FileProcessor):
    xtcePacketDefinition: Path

    def __init__(self, work_folder: Path, input_manager: InputManager) -> None:
        self.__work_folder = work_folder
        self.__input_manager = input_manager

    def is_supported(self, file: Path) -> bool:
        return file.suffix in [".pkts", ".bin"]

    def initialize(self, packet_definition: Path) -> None:
        pythonModuleRelativePath = (
            Path(os.path.dirname(__file__)).parent / packet_definition
        )
        defaultFallbackPath = Path("tlm.xml")

        logger.debug(
            "Trying XTCE packet definition file from these paths in turn: \n  %s\n  %s\n  %s\n",
            packet_definition,
            pythonModuleRelativePath,
            defaultFallbackPath,
        )

        # First try the file path as is, then in the same directory as the module, then fallback to a default.
        if packet_definition is not None and packet_definition.exists():
            logger.debug(
                f"Using XTCE packet definition file from relative path: {packet_definition!s}",
            )
            self.xtcePacketDefinition = packet_definition

        # Otherwise try path relative to the module.
        elif pythonModuleRelativePath.exists():
            logger.debug(
                f"Using XTCE packet definition file from module path: {pythonModuleRelativePath!s}",
            )
            self.xtcePacketDefinition = pythonModuleRelativePath

        else:
            logger.debug(
                f"Using XTCE packet definition file from default path: {defaultFallbackPath!s}",
            )
            self.xtcePacketDefinition = defaultFallbackPath

        if not self.xtcePacketDefinition.exists():
            raise FileNotFoundError(
                f"XTCE packet definition file not found: {packet_definition!s}"
            )

    def process(self, files: Path | list[Path]) -> dict[Path, IFilePathHandler]:
        """Process HK with XTCE tools and create CSV file."""

        if isinstance(files, Path):
            files = [files]

        # Process each file individually, and combine the results
        # into a single dict.
        combined_results: dict[int, xr.DataArray] = self.__load_and_decommutate_files(
            files
        )

        # Split each ApID into a separate file per day.
        processed_files: dict[Path, IFilePathHandler] = {}

        for apid, data in combined_results.items():
            hk_packet: str = HKPacket.from_apid(apid).packet
            path_handler = HKPathHandler(
                level=HKLevel.l1.value,
                descriptor=HKPathHandler.convert_packet_to_descriptor(hk_packet),
                content_date=None,
                extension="csv",
            )

            dataframe: pd.DataFrame = data.to_dataframe()

            # Split dataframe by day.
            dates: list[date] = TimeConversion.convert_j2000ns_to_date(
                dataframe.index.values
            )
            logger.info(
                f"Splitting data for ApID {apid} ({hk_packet}) into separate files for each day:\n"
                f"{', '.join(d.strftime('%Y%m%d') for d in sorted(set(dates)))}"
            )

            for day_info, daily_data in dataframe.groupby(dates):
                day: date = day_info[0] if isinstance(day_info, tuple) else day_info

                existing_data: pd.DataFrame | None = self.__load_existing_data(
                    apid, hk_packet, day
                )

                if existing_data is not None:
                    logger.debug(
                        f"Merging new data with existing data for {day.strftime('%Y-%m-%d')}."
                    )
                    daily_data = pd.concat([existing_data, daily_data])
                else:
                    logger.debug(
                        f"No existing data found for {day.strftime('%Y-%m-%d')}, creating new file."
                    )

                file_path, path_handler = self.__save_daily_data(
                    day, daily_data, path_handler
                )
                processed_files[file_path] = path_handler

        return processed_files

    def __load_and_decommutate_files(
        self, files: list[Path]
    ) -> dict[int, xr.DataArray]:
        combined_results: dict[int, xr.DataArray] = dict()

        for file in track(files, description="Processing HK files..."):
            file_results: dict[int, xr.DataArray] = self.__decommutate_packets(file)
            logger.info(
                f"Found {len(file_results.keys())} ApIDs ({', '.join(str(key) for key in file_results.keys())}) in {file}."
            )

            for apid, data in file_results.items():
                if apid in combined_results:
                    combined_results[apid] = xr.concat(
                        [combined_results[apid], data], dim="epoch"
                    )
                else:
                    combined_results[apid] = data

        return combined_results

    def __decommutate_packets(self, file: Path) -> dict[int, xr.DataArray]:
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
                {
                    key: (
                        "epoch",
                        value,
                    )
                    for key, value in data.items()
                },
                coords={"epoch": time_data},
            )
            ds = ds.sortby("epoch")

            dataset_dict[apid] = ds

        return dataset_dict

    def __load_existing_data(
        self, apid: int, hk_packet: str, day: date
    ) -> pd.DataFrame | None:
        l0_path_handler = HKPathHandler(
            level=HKLevel.l0.value,
            descriptor=HKPathHandler.convert_packet_to_descriptor(hk_packet),
            content_date=datetime.combine(day, datetime.min.time()),
            extension="pkts",
        )

        latest_files: list[Path] = self.__input_manager.get_all_file_versions(
            l0_path_handler, throw_if_none_found=False
        )

        if not latest_files:
            return None

        logging.info(
            f"Found {len(latest_files)} existing files for {hk_packet} on {day.strftime('%Y-%m-%d')}."
        )

        existing_data: dict[int, xr.DataArray] = self.__load_and_decommutate_files(
            latest_files
        )

        # Only data from this ApID should have been loaded.
        assert apid in existing_data
        assert len(existing_data) == 1

        return existing_data[apid].to_dataframe()

    def __save_daily_data(
        self, day: date, daily_data: pd.DataFrame, path_handler: HKPathHandler
    ) -> tuple[Path, HKPathHandler]:
        logger.debug(f"Generating file for {day.strftime('%Y-%m-%d')}.")

        path_handler.content_date = datetime.combine(day, datetime.min.time())
        file_path = self.__work_folder / path_handler.get_filename()

        # Save to CSV.
        daily_data.drop_duplicates(subset="shcoarse", keep="last", inplace=True)
        daily_data.sort_index(inplace=True)
        daily_data.to_csv(file_path)

        # Use a deep-copy, otherwise the same handle will be used for all files.
        return file_path, deepcopy(path_handler)
