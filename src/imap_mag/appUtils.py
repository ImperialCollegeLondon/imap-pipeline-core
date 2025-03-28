import logging
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

import numpy as np

from imap_mag import appConfig
from imap_mag.config.FetchMode import FetchMode

from .DB import DatabaseFileOutputManager
from .outputManager import IFileMetadataProvider, IOutputManager, OutputManager

logger = logging.getLogger(__name__)

IMAP_EPOCH = np.datetime64("2010-01-01T00:00:00", "ns")
J2000_EPOCH = np.datetime64("2000-01-01T11:58:55.816", "ns")

APID_TO_PACKET: dict[int, str] = {
    1028: "MAG_HSK_SID1",
    1055: "MAG_HSK_SID2",
    1063: "MAG_HSK_PW",
    1064: "MAG_HSK_STATUS",
    1082: "MAG_HSK_SCI",
    # 1051: "MAG_HSK_SID11", SID11 is not supported by WebPODA
    1060: "MAG_HSK_SID12",
    1053: "MAG_HSK_SID15",
    1054: "MAG_HSK_SID16",
    1045: "MAG_HSK_SID20",
}

HKPacket = Enum("HKPacket", [(value, value) for value in APID_TO_PACKET.values()])  # type: ignore
HK_PACKETS: list[str] = [e.value for e in HKPacket]  # type: ignore


def convertMETToJ2000ns(
    met: np.typing.ArrayLike,
    reference_epoch: np.datetime64 = IMAP_EPOCH,
) -> np.typing.ArrayLike:
    """Convert mission elapsed time (MET) to nanoseconds from J2000."""
    time_array = (np.asarray(met, dtype=float) * 1e9).astype(np.int64)
    j2000_offset = (
        (reference_epoch - J2000_EPOCH).astype("timedelta64[ns]").astype(np.int64)
    )
    return j2000_offset + time_array


def getPacketFromApID(apid: int) -> str:
    """Get packet name from ApID."""
    if apid not in APID_TO_PACKET:
        logger.critical(f"ApID {apid} does not match any known packet.")
        raise ValueError(f"ApID {apid} does not match any known packet.")
    return APID_TO_PACKET[apid]


def forceUTCTimeZone(date: datetime) -> datetime:
    """Convert given datetime objects to UTC timezone and remove timezone."""
    return date.astimezone(timezone.utc).replace(tzinfo=None)


# TODO: Replace all uses of this with getOutputManagerByMode version
def getOutputManager(destination: appConfig.Destination) -> IOutputManager:
    """Retrieve output manager based on destination."""

    output_manager: IOutputManager = OutputManager(destination.folder)

    if destination.export_to_database:
        output_manager = DatabaseFileOutputManager(output_manager)

    return output_manager


def getOutputManagerByMode(destination_folder: Path, mode: FetchMode) -> IOutputManager:
    """Retrieve output manager based on destination and mode."""

    if mode == FetchMode.DownloadOnly:
        return OutputManager(destination_folder)
    elif mode == FetchMode.DownloadAndUpdateProgress:
        return DatabaseFileOutputManager(OutputManager(destination_folder))
    else:
        raise ValueError(f"Unsupported mode: {mode}")


def copyFileToDestination(
    file_path: Path,
    destination: appConfig.Destination,
    output_manager: Optional[OutputManager] = None,
) -> tuple[Path, IFileMetadataProvider]:
    """Copy file to destination folder."""

    class SimpleMetadataProvider(IFileMetadataProvider):
        """Simple metadata provider for compatibility."""

        def __init__(self, filename: str) -> None:
            self.filename = filename

        def supports_versioning(self) -> bool:
            return False

        def get_folder_structure(self) -> str:
            return ""

        def get_filename(self) -> str:
            return self.filename

    destination_folder = Path(destination.folder)

    if output_manager is None:
        output_manager = OutputManager(destination_folder)

    return output_manager.add_file(
        file_path, SimpleMetadataProvider(destination.filename)
    )


class DatetimeProvider:
    """Datetime provider to remove dependency on `datetime` library."""

    @staticmethod
    def now() -> datetime:
        return datetime.now()

    @staticmethod
    def today(type=datetime) -> date:
        return type.today()

    @staticmethod
    def tomorrow(type=datetime) -> date:
        return type.today().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(days=1)

    @staticmethod
    def yesterday(type=datetime) -> date:
        return type.today().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)

    @staticmethod
    def end_of_today() -> datetime:
        return datetime.today().replace(
            hour=23, minute=59, second=59, microsecond=999999
        )
