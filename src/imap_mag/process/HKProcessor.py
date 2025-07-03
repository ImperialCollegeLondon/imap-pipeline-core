import collections
import logging
import os
import re
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import xarray as xr
from rich.progress import track
from space_packet_parser import definitions

from imap_mag.io import HKMetadataProvider, IFileMetadataProvider
from imap_mag.process.FileProcessor import FileProcessor
from imap_mag.util import HKLevel, HKPacket, TimeConversion

logger = logging.getLogger(__name__)


class HKProcessor(FileProcessor):
    xtcePacketDefinition: Path

    def __init__(self, work_folder: Path) -> None:
        self.__work_folder = work_folder

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

    def process(self, files: Path | list[Path]) -> dict[Path, IFileMetadataProvider]:
        """Process HK with XTCE tools and create CSV file."""

        if isinstance(files, Path):
            files = [files]

        # Process each file individually, and combine the results
        # into a single dict.
        combined_results: dict[int, xr.DataArray] = dict()

        for file in track(files, description="Processing HK files..."):
            file_results: dict[int, xr.DataArray] = self.__do_process(file)
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

        # Split each ApID into a separate file per day.
        processed_files: dict[Path, IFileMetadataProvider] = {}

        for apid, data in combined_results.items():
            hk_packet: str = HKPacket.from_apid(apid).packet
            metadata_provider = HKMetadataProvider(
                level=HKLevel.l1.value,
                descriptor=hk_packet.lower().strip("mag_").replace("_", "-"),
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

            for day, daily_data in dataframe.groupby(dates):
                day = day[0] if isinstance(day, tuple) else day
                logger.debug(f"Generating file for {day.strftime('%Y-%m-%d')}.")  # type: ignore

                metadata_provider.content_date = datetime.combine(
                    day,  # type: ignore
                    datetime.min.time(),
                )
                file_path = self.__work_folder / metadata_provider.get_filename()

                daily_data.sort_index(inplace=False).to_csv(file_path)
                processed_files[file_path] = metadata_provider

        return processed_files

    def __do_process(self, file: Path) -> dict[int, xr.DataArray]:
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
                    data_dict[apid][
                        re.sub(r"^mag_hsk_[a-zA-Z0-9]+\.", "", key.lower())
                    ].append(value)

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
