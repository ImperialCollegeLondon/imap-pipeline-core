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
from imap_mag.util import BinaryHelper, HKLevel, HKPacket, TimeConversion

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

    def __init__(self, work_folder: Path, input_manager: InputManager) -> None:
        self.__work_folder = work_folder
        self.__input_manager = input_manager

    def is_supported(self, file: Path) -> bool:
        return file.suffix in [".pkts", ".bin"]

    def initialize(self, packet_definition: Path) -> None:
        paths_to_try: dict[str, Path] = {
            "relative": packet_definition,
            "module": Path(os.path.dirname(__file__)).parent / packet_definition,
            "default": Path("tlm.xml"),
        }

        paths_to_try_string: str = "\n".join(
            [f"{source}: {path}" for source, path in paths_to_try.items()]
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
        apids_by_day: dict[int, set[date]] = dict()

        for file in files:
            file_apids: dict[int, set[date]] = BinaryHelper.get_apids_and_days(file)

            for apid, days in file_apids.items():
                apids_by_day.setdefault(apid, set()).update(days)

        logger.info(
            f"Found {len(apids_by_day)} ApIDs in {len(files)} files:\n{', '.join(str(apid) for apid in apids_by_day.keys())}"
        )

        # Load data for each ApID.
        results: dict[int, pd.DataFrame] = dict()

        for apid, days in apids_by_day.items():
            hk_packet: str = HKPacket.from_apid(apid).packet
            logger.info(
                f"Processing ApID {apid} ({hk_packet}) for days:\n{', '.join(d.strftime('%Y-%m-%d') for d in sorted(days))}"
            )
            results.setdefault(apid, pd.DataFrame())

            for day in days:
                existing_data: pd.DataFrame | None = self.__load_datastore_data(
                    apid, hk_packet, day
                )
                if existing_data is not None:
                    results[apid] = add_or_concat_dataframe(
                        results, apid, existing_data
                    )

        # If original files are not in the datastore, load them.
        # This data is loaded last, such that it overrides existing data with same SHCOARSE.
        datastore_path = self.__input_manager.location.absolute().as_posix()
        new_files: list[Path] = [
            file for file in files if datastore_path not in file.absolute().as_posix()
        ]
        logger.info(
            f"Loading {len(new_files)} new files that are not in the datastore:\n{', '.join(str(file) for file in new_files)}"
        )

        new_data: dict[int, pd.DataFrame] = self.__load_and_decommutate_files(new_files)

        for apid, df in new_data.items():
            results[apid] = add_or_concat_dataframe(results, apid, df)

        # Split each ApID into a separate file per day.
        processed_files: dict[Path, IFilePathHandler] = {}

        for apid, data in results.items():
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

    def __load_datastore_data(
        self, apid: int, hk_packet: str, day: date
    ) -> pd.DataFrame | None:
        l0_path_handler = HKPathHandler(
            level=HKLevel.l0.value,
            descriptor=HKPathHandler.convert_packet_to_descriptor(hk_packet),
            content_date=datetime.combine(day, datetime.min.time()),
            extension="pkts",
        )

        existing_files: list[Path] = self.__input_manager.get_all_file_versions(
            l0_path_handler, throw_if_not_found=False
        )

        if not existing_files:
            return None
        else:
            logger.info(
                f"Found {len(existing_files)} existing files for {hk_packet} on {day.strftime('%Y-%m-%d')}."
            )

            existing_data: dict[int, pd.DataFrame] = self.__load_and_decommutate_files(
                existing_files
            )
            assert (apid in existing_data) and (len(existing_data) == 1)

            return existing_data[apid]

    def __load_and_decommutate_files(
        self, files: list[Path]
    ) -> dict[int, pd.DataFrame]:
        combined: dict[int, pd.DataFrame] = dict()

        for file in track(files, description="Processing HK files..."):
            results: dict[int, xr.DataArray] = self.__decommutate_packets(file)
            logger.info(
                f"Found {len(results.keys())} ApIDs ({', '.join(str(key) for key in results.keys())}) in {file}."
            )

            for apid, data in results.items():
                combined[apid] = add_or_concat_dataframe(
                    combined, apid, data.to_dataframe()
                )

        return combined

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
                {key: ("epoch", value) for key, value in data.items()},
                coords={"epoch": time_data},
            )
            ds = ds.sortby("epoch")

            dataset_dict[apid] = ds

        return dataset_dict

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
