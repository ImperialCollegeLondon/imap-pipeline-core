import logging
from pathlib import Path
from typing import overload

from imap_mag.process.FileProcessor import FileProcessor
from imap_mag.process.HKProcessor import HKProcessor

logger = logging.getLogger(__name__)


@overload
def dispatch(file: Path, work_folder: Path) -> FileProcessor:
    pass


@overload
def dispatch(file: list[Path], work_folder: Path) -> FileProcessor:
    pass


def dispatch(file: Path | list[Path], work_folder: Path) -> FileProcessor:
    """Dispatch a file or a list of files to the appropriate processor."""

    if isinstance(file, list):
        file = file[0]

    match file.suffix:
        case ".pkts" | ".bin":
            logger.info(f"File {file} contains HK.")
            return HKProcessor(work_folder)
        case _:
            logger.error(
                f"File {file} contains unknown data. File suffix {file.suffix} cannot be processed."
            )
            raise NotImplementedError(
                f"File {file} contains unknown data. File suffix {file.suffix} cannot be processed."
            )
