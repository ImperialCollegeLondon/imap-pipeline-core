import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np

from imap_mag import appConfig

from .DB import DatabaseFileOutputManager
from .outputManager import IFileMetadataProvider, IOutputManager, OutputManager

logger = logging.getLogger(__name__)

IMAP_EPOCH = np.datetime64("2010-01-01T00:00:00", "ns")
J2000_EPOCH = np.datetime64("2000-01-01T11:58:55.816", "ns")

HK_APIDS: list[int] = [
    1028,
    1055,
    1063,
    1064,
    1082,
    1060,
    1053,
    1054,
    1045,
]
APID_TO_PACKET: dict[int, str] = {
    1028: "MAG_HSK_SID1",
    1055: "MAG_HSK_SID2",
    1063: "MAG_HSK_PW",
    1064: "MAG_HSK_STATUS",
    1082: "MAG_HSK_SCI",
    1051: "MAG_HSK_SID11",
    1060: "MAG_HSK_SID12",
    1053: "MAG_HSK_SID15",
    1054: "MAG_HSK_SID16",
    1045: "MAG_HSK_SID20",
}


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


def forceUTCTimeZone(*args: datetime) -> tuple[datetime, ...]:
    """Convert given datetime objects to UTC timezone and remove timezone."""
    return tuple(arg.astimezone(timezone.utc).replace(tzinfo=None) for arg in args)


def getOutputManager(destination: appConfig.Destination) -> IOutputManager:
    """Retrieve output manager based on destination."""

    output_manager: IOutputManager = OutputManager(destination.folder)

    if destination.export_to_database:
        output_manager = DatabaseFileOutputManager(output_manager)

    return output_manager


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
