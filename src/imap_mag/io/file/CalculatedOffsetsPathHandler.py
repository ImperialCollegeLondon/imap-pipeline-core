import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler

logger = logging.getLogger(__name__)

# The two datastore sub-folders (under calibration/calculated_offsets) that hold the
# per-day spin-plane offset CSVs produced by the scripted-L2 MATLAB calibration.
SPIN_PLANE = "spin_plane"  # spin-averaged offsets
SPIN_OPTIMISED = "spin_optimised"  # spin-tone-corrected ("optimised") offsets
OFFSET_TYPES = (SPIN_PLANE, SPIN_OPTIMISED)

# Each offset type has its own file-name descriptor so the two products are uniquely
# named (and not just distinguished by their folder). This mirrors what MATLAB writes.
_DESCRIPTOR_BY_OFFSET_TYPE = {
    SPIN_PLANE: "spin-plane-offsets",
    SPIN_OPTIMISED: "spin-plane-optimised-offsets",
}
_OFFSET_TYPE_BY_DESCRIPTOR = {v: k for k, v in _DESCRIPTOR_BY_OFFSET_TYPE.items()}


@dataclass
class CalculatedOffsetsPathHandler(VersionedPathHandler):
    """Path handler for calculated spin-plane offset CSVs.

    These live in ``calibration/calculated_offsets/<offset_type>/`` where
    ``offset_type`` is ``spin_plane`` (spin-averaged offsets) or ``spin_optimised``
    (spin-tone-corrected offsets). Each offset type has a distinct file-name
    descriptor so the two products are uniquely named:

    - ``spin_plane``     -> ``imap_mag_<sensor>-spin-plane-offsets_<date>_vNNN.csv``
    - ``spin_optimised`` -> ``imap_mag_<sensor>-spin-plane-optimised-offsets_<date>_vNNN.csv``
    """

    mission: str = "imap"
    instrument: str = "mag"
    sensor: str | None = None  # "mago" or "magi"
    offset_type: str | None = None  # one of OFFSET_TYPES
    content_date: datetime | None = None  # date the offsets belong to
    extension: str = "csv"

    def _descriptor(self) -> str:
        super()._check_property_values("descriptor", ["offset_type"])
        assert self.offset_type

        if self.offset_type not in _DESCRIPTOR_BY_OFFSET_TYPE:
            raise ValueError(
                f"Unknown offset_type '{self.offset_type}'. Expected one of "
                f"{OFFSET_TYPES}."
            )
        return _DESCRIPTOR_BY_OFFSET_TYPE[self.offset_type]

    def get_content_date_for_indexing(self) -> datetime | None:
        return self.content_date

    def get_folder_structure(self) -> str:
        super()._check_property_values("folder structure", ["offset_type"])
        assert self.offset_type

        return (
            Path("calibration") / "calculated_offsets" / self.offset_type
        ).as_posix()

    def get_filename(self) -> str:
        super()._check_property_values(
            "file name", ["sensor", "offset_type", "content_date"]
        )
        assert self.sensor and self.content_date

        return (
            f"{self.mission}_{self.instrument}_{self.sensor}-{self._descriptor()}_"
            f"{self.content_date.strftime('%Y%m%d')}_v{self.version:03d}.{self.extension}"
        )

    def get_unsequenced_pattern(self) -> re.Pattern:
        super()._check_property_values(
            "pattern", ["sensor", "offset_type", "content_date"]
        )
        assert self.sensor and self.content_date

        return re.compile(
            rf"{self.mission}_{self.instrument}_{re.escape(self.sensor)}-"
            rf"{re.escape(self._descriptor())}_{self.content_date.strftime('%Y%m%d')}"
            rf"_v(?P<version>\d+)\.{self.extension}"
        )

    @classmethod
    def from_filename(
        cls, filename: str | Path
    ) -> "CalculatedOffsetsPathHandler | None":
        """Instantiate from a file name.

        The descriptor uniquely identifies the offset type, so ``offset_type`` is set
        (``spin-plane-optimised-offsets`` -> ``spin_optimised``, ``spin-plane-offsets``
        -> ``spin_plane``).
        """
        # Match the more specific (optimised) descriptor first.
        descriptors = "|".join(re.escape(d) for d in _OFFSET_TYPE_BY_DESCRIPTOR)
        match = re.match(
            rf"imap_mag_(?P<sensor>mago|magi)-(?P<descr>{descriptors})_"
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
            offset_type=_OFFSET_TYPE_BY_DESCRIPTOR[match["descr"]],
            content_date=datetime.strptime(match["date"], "%Y%m%d"),
            version=int(match["version"]),
            extension=match["ext"],
        )

    @classmethod
    def from_work_folder_file(cls, file: Path) -> "CalculatedOffsetsPathHandler":
        """Build a handler for a MATLAB-produced offsets CSV in the work folder.

        The offset type is derived from the file name; the immediate parent folder
        (``spin_plane`` / ``spin_optimised``) is cross-checked against it as a guard
        against a misplaced file.
        """
        handler = cls.from_filename(file.name)
        if handler is None:
            raise ValueError(
                f"'{file.name}' is not a recognised calculated-offsets file name."
            )

        folder = file.parent.name
        if folder not in OFFSET_TYPES:
            raise ValueError(
                f"Offsets file {file} must live in one of {OFFSET_TYPES} folders, "
                f"got '{folder}'."
            )
        if handler.offset_type != folder:
            raise ValueError(
                f"Offsets file {file} name implies offset type "
                f"'{handler.offset_type}' but it sits in the '{folder}' folder."
            )

        return handler
