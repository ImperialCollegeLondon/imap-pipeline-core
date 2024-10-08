import logging
import os
import shutil
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import typer

from . import appConfig

IMAP_EPOCH = np.datetime64("2010-01-01T00:00:00", "ns")
J2000_EPOCH = np.datetime64("2000-01-01T11:58:55.816", "ns")

APID_TO_PACKET = {
    1028: "MAG_HSK_SID1",
    1055: "MAG_HSK_SID2",
    1063: "MAG_HSK_PW",
    1064: "MAG_HSK_STATUS",
    1082: "MAG_HSK_SID5",
    1051: "MAG_HSK_SID11",
    1060: "MAG_HSK_SID12",
    1053: "MAG_HSK_SID15",
    1054: "MAG_HSK_SID16",
    1045: "MAG_HSK_SID20",
}


def convertMETToJ2000ns(
    met: np.typing.ArrayLike,
    reference_epoch: Optional[np.datetime64] = IMAP_EPOCH,
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
        logging.critical(f"ApID {apid} does not match any known packet.")
        raise typer.Abort()
    return APID_TO_PACKET[apid]


def convertToDatetime(string: str) -> np.datetime64:
    """Convert string to datetime."""
    try:
        return pd.to_datetime(string)
    except Exception as e:
        logging.critical(f"Error parsing {string} as datetime: {e}")
        raise typer.Abort()


def copyFileToDestination(filePath: Path, destination: appConfig.Destination) -> None:
    """Copy file to destination folder."""

    destinationFile = Path(destination.folder)

    if not destinationFile.exists():
        logging.debug(f"Creating destination folder {destinationFile}.")
        os.makedirs(destinationFile)

    if destination.filename:
        destinationFile = destinationFile / destination.filename

    logging.info(f"Copying {filePath} to {destinationFile.absolute()}")
    completed = shutil.copy2(filePath, destinationFile)
    logging.info(f"Copy complete: {completed}")
