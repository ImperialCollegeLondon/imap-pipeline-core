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

    available_processor_types: list[type] = [
        HKProcessor,
    ]

    if isinstance(file, list):
        file = file[0]

    for processor_type in available_processor_types:
        processor = processor_type(work_folder)

        if processor.is_supported(file):
            logger.info(f"File {file} is supported by {processor_type.__name__}.")
            return processor

    logger.error(f"File {file} is not supported and cannot be processed.")
    raise NotImplementedError(f"File {file} is not supported and cannot be processed.")
