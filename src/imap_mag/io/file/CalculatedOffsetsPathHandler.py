import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import ClassVar

from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler

logger = logging.getLogger(__name__)

# The two datastore sub-folders (under calibration/calculated_offsets) that hold the
# per-day spin-plane offset CSVs produced by the scripted-L2 MATLAB calibration.
SPIN_PLANE = "spin_plane"  # spin-averaged offsets
SPIN_OPTIMISED = "spin_optimised"  # spin-tone-corrected ("optimised") offsets
OFFSET_TYPES = (SPIN_PLANE, SPIN_OPTIMISED)


@dataclass
class CalculatedOffsetsPathHandler(VersionedPathHandler):
    """Path handler for calculated spin-plane offset CSVs.

    These live in ``calibration/calculated_offsets/<offset_type>/`` where
    ``offset_type`` is ``spin_plane`` (spin-averaged offsets) or ``spin_optimised``
    (spin-tone-corrected offsets). The scripted-L2 MATLAB calibration writes one CSV
    per sensor per day into each folder. The file name is identical in both folders,
    so the folder (``offset_type``) is what distinguishes the two products.

    File names look like ``imap_mag_mago-spin-plane-offsets_20260114_v024.csv``.
    """

    mission: str = "imap"
    instrument: str = "mag"
    sensor: str | None = None  # "mago" or "magi"
    offset_type: str | None = None  # one of OFFSET_TYPES
    content_date: datetime | None = None  # date the offsets belong to
    extension: str = "csv"

    DESCRIPTOR: ClassVar[str] = "spin-plane-offsets"

    def get_content_date_for_indexing(self) -> datetime | None:
        return self.content_date

    def get_folder_structure(self) -> str:
        super()._check_property_values("folder structure", ["offset_type"])
        assert self.offset_type

        return (
            Path("calibration") / "calculated_offsets" / self.offset_type
        ).as_posix()

    def get_filename(self) -> str:
        super()._check_property_values("file name", ["sensor", "content_date"])
        assert self.sensor and self.content_date

        return (
            f"{self.mission}_{self.instrument}_{self.sensor}-{self.DESCRIPTOR}_"
            f"{self.content_date.strftime('%Y%m%d')}_v{self.version:03d}.{self.extension}"
        )

    def get_unsequenced_pattern(self) -> re.Pattern:
        super()._check_property_values("pattern", ["sensor", "content_date"])
        assert self.sensor and self.content_date

        return re.compile(
            rf"{self.mission}_{self.instrument}_{re.escape(self.sensor)}-"
            rf"{re.escape(self.DESCRIPTOR)}_{self.content_date.strftime('%Y%m%d')}"
            rf"_v(?P<version>\d+)\.{self.extension}"
        )

    @classmethod
    def from_filename(
        cls, filename: str | Path
    ) -> "CalculatedOffsetsPathHandler | None":
        """Instantiate from a file name.

        The name alone cannot tell ``spin_plane`` from ``spin_optimised`` (both use
        identical names in different folders), so ``offset_type`` is left unset. Use
        :meth:`from_work_folder_file` when the containing folder is known.
        """
        match = re.match(
            rf"imap_mag_(?P<sensor>mago|magi)-{re.escape(cls.DESCRIPTOR)}_"
            r"(?P<date>\d{8})_v(?P<version>\d+)\.(?P<ext>\w+)",
            Path(filename).name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with calculated-offsets regex."
        )

        if match is None:
            return None

        return cls(
            sensor=match["sensor"],
            content_date=datetime.strptime(match["date"], "%Y%m%d"),
            version=int(match["version"]),
            extension=match["ext"],
        )

    @classmethod
    def from_work_folder_file(cls, file: Path) -> "CalculatedOffsetsPathHandler":
        """Build a handler for a MATLAB-produced offsets CSV in the work folder.

        ``offset_type`` is taken from the immediate parent folder name (``spin_plane``
        or ``spin_optimised``); the remaining fields are parsed from the file name.
        """
        handler = cls.from_filename(file.name)
        if handler is None:
            raise ValueError(
                f"'{file.name}' is not a recognised calculated-offsets file name."
            )

        offset_type = file.parent.name
        if offset_type not in OFFSET_TYPES:
            raise ValueError(
                f"Offsets file {file} must live in one of {OFFSET_TYPES} folders, "
                f"got '{offset_type}'."
            )

        handler.offset_type = offset_type
        return handler
