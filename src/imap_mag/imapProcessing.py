import abc
import collections
import logging
import os
import re
from pathlib import Path

import xarray as xr
from space_packet_parser import definitions

from . import appConfig, appUtils

logger = logging.getLogger(__name__)


class FileProcessor(abc.ABC):
    """Interface for IMAP processing."""

    @abc.abstractmethod
    def initialize(self, config: appConfig.AppConfig) -> None:
        pass

    @abc.abstractmethod
    def process(self, file: Path) -> Path:
        pass


class ScienceProcessor(FileProcessor):
    def initialize(self, config: appConfig.AppConfig) -> None:
        pass

    def process(self, file: Path) -> Path:
        return file


class HKProcessor(FileProcessor):
    xtcePacketDefinition: Path

    def initialize(self, config: appConfig.AppConfig) -> None:
        # first try the file path as is, then in the same directory as the module, then fallback to a default
        pythonModuleRelativePath = Path(
            os.path.join(os.path.dirname(__file__), config.packet_definition.hk)
        )
        defaultFallbackPath = Path("tlm.xml")
        logger.debug(
            "Trying XTCE packet definition file from these paths in turn: \n  %s\n  %s\n  %s\n",
            config.packet_definition.hk,
            pythonModuleRelativePath,
            defaultFallbackPath,
        )
        if (
            config.packet_definition is not None
            and config.packet_definition.hk is not None
            and config.packet_definition.hk.exists()
        ):
            logger.debug(
                "Using XTCE packet definition file from relative path: %s",
                config.packet_definition.hk,
            )
            self.xtcePacketDefinition = config.packet_definition.hk
        # otherwise try path relative to the module
        elif pythonModuleRelativePath.exists():
            logger.debug(
                "Using XTCE packet definition file from module path: %s",
                pythonModuleRelativePath,
            )
            self.xtcePacketDefinition = pythonModuleRelativePath
        else:
            logger.debug(
                "Using XTCE packet definition file from default path: %s",
                defaultFallbackPath,
            )
            self.xtcePacketDefinition = defaultFallbackPath

        if not self.xtcePacketDefinition.exists():
            raise FileNotFoundError(
                f"XTCE packet definition file not found: {config.packet_definition.hk}"
            )

    def process(self, file: Path) -> Path:
        """Process HK with XTCE tools and create CSV file."""

        # Extract data from binary file.
        dataDict: dict[int, dict] = dict()

        packetDefinition = definitions.XtcePacketDefinition(self.xtcePacketDefinition)

        with open(file, "rb") as binaryData:
            packetGenerator = packetDefinition.packet_generator(
                binaryData, show_progress=True
            )

            for packet in packetGenerator:
                apid = packet.header["PKT_APID"].raw_value
                dataDict.setdefault(apid, collections.defaultdict(list))

                packetContent = packet.user_data | packet.header

                for key, value in packetContent.items():
                    if value is None:
                        value = value.raw_value
                    elif hasattr(value, "decode"):
                        value = int.from_bytes(value, byteorder="big")
                    dataDict[apid][
                        re.sub(r"^mag_hsk_[a-zA-Z0-9]+\.", "", key.lower())
                    ].append(value)

        # Convert data to xarray datasets.
        datasetDict = {}

        for apid, data in dataDict.items():
            time_key = next(iter(data.keys()))
            time_data = appUtils.convertMETToJ2000ns(data[time_key])

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

            datasetDict[apid] = ds

        # Write CSV files.
        for apid, dataset in datasetDict.items():
            csvFile = file.with_suffix(".csv")
            dataset.to_dataframe().to_csv(csvFile)

            # TODO: What about the other ApIDs?
            return csvFile

        # No data found.
        return file


class UnknownProcessor(FileProcessor):
    def initialize(self, config: appConfig.AppConfig) -> None:
        pass

    def process(self, file: Path) -> Path:
        return file


def dispatchFile(file: Path) -> FileProcessor:
    match file.suffix:
        case ".cdf":
            logger.info(f"File {file} contains science.")
            return ScienceProcessor()
        case ".pkts" | ".bin":
            logger.info(f"File {file} contains HK.")
            return HKProcessor()
        case _:
            logger.info(f"File {file} contains unknown data.")
            return UnknownProcessor()
