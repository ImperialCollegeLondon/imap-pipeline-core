import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler

logger = logging.getLogger(__name__)

# The datastore sub-folders (under calibration/calculated_offsets) that hold the
# per-day offset CSVs produced by the various MATLAB calibration tools.
SPIN_PLANE = "spin_plane"  # spin-averaged offsets, from the scripted-L2 calibration
SPIN_OPTIMISED = "spin_optimised"  # spin-tone-corrected ("optimised") offsets
LEINWEBER = "leinweber"  # solar-wind offsets, from generate_raw_leinweber_offset
OFFSET_TYPES = (SPIN_PLANE, SPIN_OPTIMISED, LEINWEBER)

# Each offset type has its own file-name descriptor so the products are uniquely
# named (and not just distinguished by their folder). This mirrors what MATLAB writes.
# Leinweber's descriptor also embeds the window duration (in hours), so it is built
# dynamically in _descriptor() rather than listed here.
_DESCRIPTOR_BY_OFFSET_TYPE = {
    SPIN_PLANE: "spin-plane-offsets",
    SPIN_OPTIMISED: "spin-plane-optimised-offsets",
}
_OFFSET_TYPE_BY_DESCRIPTOR = {v: k for k, v in _DESCRIPTOR_BY_OFFSET_TYPE.items()}

_LEINWEBER_DESCRIPTOR_PATTERN = re.compile(
    r"^raw-leinweber-offsets-(?P<duration>\d+)hr$"
)


@dataclass
class CalculatedOffsetsPathHandler(VersionedPathHandler):
    """Path handler for calculated spin-plane offset CSVs.

    These live in ``calibration/calculated_offsets/<offset_type>/`` where
    ``offset_type`` is ``spin_plane`` (spin-averaged offsets) or ``spin_optimised``
    (spin-tone-corrected offsets). Each offset type has a distinct file-name
    descriptor so the two products are uniquely named:

    - ``spin_plane``     -> ``imap_mag_<sensor>-spin-plane-offsets_<date>_vNNN.csv``
    - ``spin_optimised`` -> ``imap_mag_<sensor>-spin-plane-optimised-offsets_<date>_vNNN.csv``
    - ``leinweber``      -> ``imap_mag_<sensor>-raw-leinweber-offsets-<duration>hr_<date>_vNNN.csv``
    """

    mission: str = "imap"
    instrument: str = "mag"
    sensor: str | None = None  # "mago" or "magi"
    offset_type: str | None = None  # one of OFFSET_TYPES
    content_date: datetime | None = None  # date the offsets belong to
    duration_hours: int | None = None  # leinweber only: window width, in hours
    extension: str = "csv"

    def _descriptor(self) -> str:
        super()._check_property_values("descriptor", ["offset_type"])
        assert self.offset_type

        if self.offset_type == LEINWEBER:
            super()._check_property_values("descriptor", ["duration_hours"])
            assert self.duration_hours is not None
            return f"raw-leinweber-offsets-{self.duration_hours}hr"

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
        -> ``spin_plane``, ``raw-leinweber-offsets-<N>hr`` -> ``leinweber``).
        """

        if isinstance(filename, str):
            filename = Path(filename)

        # Match the more specific (optimised) descriptor first. Leinweber's descriptor
        # embeds a variable duration, so it is matched separately via its own pattern.
        descriptors = "|".join(re.escape(d) for d in _OFFSET_TYPE_BY_DESCRIPTOR)
        match = re.match(
            rf"imap_mag_(?P<sensor>mago|magi)-"
            rf"(?P<descr>{descriptors}|raw-leinweber-offsets-\d+hr)_"
            r"(?P<date>\d{8})_v(?P<version>\d+)\.(?P<ext>\w+)",
            filename.name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with calculated-offsets regex."
        )

        if match is None:
            return None

        offset_type_from_folder = filename.parent.name
        if offset_type_from_folder not in OFFSET_TYPES:
            return None

        duration_hours: int | None = None
        leinweber_match = _LEINWEBER_DESCRIPTOR_PATTERN.match(match["descr"])
        if leinweber_match:
            offset_type = LEINWEBER
            duration_hours = int(leinweber_match["duration"])
        else:
            offset_type = _OFFSET_TYPE_BY_DESCRIPTOR[match["descr"]]

        if offset_type != offset_type_from_folder:
            raise ValueError(
                f"Offsets file {filename} name implies offset type "
                f"'{offset_type}' but it sits in the '{offset_type_from_folder}' folder."
            )

        return cls(
            sensor=match["sensor"],
            offset_type=offset_type,
            content_date=datetime.strptime(match["date"], "%Y%m%d"),
            duration_hours=duration_hours,
            version=int(match["version"]),
            extension=match["ext"],
        )
