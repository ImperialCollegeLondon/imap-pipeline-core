import logging
import re
import typing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler
from imap_mag.util import TimeConversion

logger = logging.getLogger(__name__)

T = typing.TypeVar("T", bound="SpinTablePathHandler")

"""
Spin table files from the SDC API have paths like:
    imap/spice/spin/imap_2026_089_2026_090_01.spin

They are saved under the spice folder in the datastore:
    [DATASTORE]/spice/spin/imap_2026_089_2026_090_01.spin

The filename pattern is: imap_{start_year}_{start_doy}_{end_year}_{end_doy}_{version}.spin
"""

SPIN_TABLE_FILENAME_PATTERN = re.compile(
    r"^imap_(\d{4})_(\d{3})_(\d{4})_(\d{3})_(\d{2})\.spin$"
)


@dataclass
class SpinTablePathHandler(VersionedPathHandler):
    """Path handler for spin table files."""

    filename: str | None = None
    metadata: dict | None = None
    content_date: datetime | None = None

    @staticmethod
    def get_root_folder() -> str:
        return "spice"

    def supports_sequencing(self) -> bool:
        return False

    def get_unsequenced_pattern(self) -> re.Pattern:
        raise ValueError(
            "Spin table files do not support sequencing. "
            "Versions are fixed by the source."
        )

    def get_content_date_for_indexing(self) -> datetime | None:
        return self.content_date

    def get_folder_structure(self) -> str:
        return (Path(self.get_root_folder()) / "spin").as_posix()

    def get_filename(self) -> str:
        super()._check_property_values("get_filename", ["filename"])
        assert self.filename
        return self.filename

    def add_metadata(self, metadata: dict) -> None:
        self.content_date = TimeConversion.try_extract_iso_like_datetime(
            metadata, "start_date"
        ) or TimeConversion.try_extract_iso_like_datetime(metadata, "ingestion_date")

        self.metadata = metadata

        if metadata.get("version") is not None:
            self.version = int(metadata["version"])

    def get_metadata(self) -> dict | None:
        return self.metadata

    @classmethod
    def from_filename(cls: type[T], filename: str | Path) -> T | None:
        if filename is None:
            return None

        filename_only = (
            filename.name if isinstance(filename, Path) else Path(filename).name
        )

        match = SPIN_TABLE_FILENAME_PATTERN.match(filename_only)
        if not match:
            return None

        start_year = int(match.group(1))
        start_doy = int(match.group(2))
        version = int(match.group(5))

        content_date = datetime.strptime(f"{start_year}-{start_doy}", "%Y-%j")

        handler = cls(
            filename=filename_only,
            content_date=content_date,
        )
        handler.version = version

        logger.debug(
            f"Created SpinTablePathHandler for file {filename} with version {version}."
        )
        return handler
