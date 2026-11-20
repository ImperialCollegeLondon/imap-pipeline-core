import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.file.CalculatedOffsetsPathHandler import CalculatedOffsetsPathHandler

logger = logging.getLogger(__name__)

LEINWEBER = "leinweber"  # solar-wind offsets, from generate_raw_leinweber_offset


@dataclass
class LeinweberOffsetsPathHandler(CalculatedOffsetsPathHandler):
    """Path handler for Leinweber (solar-wind) offsets CSVs.

    These live in ``calibration/calculated_offsets/leinweber/`` and have a distinct
    file-name descriptor that embeds the window duration (in hours):

    - ``imap_mag_<sensor>-raw-leinweber-offsets-<duration>hr_<date>_vNNN.csv``
    """

    offset_type: str = LEINWEBER
    duration_hours: int | None = None  # window width, in hours

    def _descriptor(self) -> str:
        super()._check_property_values("descriptor", ["duration_hours"])
        assert self.duration_hours is not None

        return f"raw-leinweber-offsets-{self.duration_hours}hr"

    @classmethod
    def from_filename(
        cls, filename: str | Path
    ) -> "LeinweberOffsetsPathHandler | None":
        """Instantiate from a file name."""

        if isinstance(filename, str):
            filename = Path(filename)

        match = re.match(
            r"imap_mag_(?P<sensor>mago|magi)-raw-leinweber-offsets-(?P<duration>\d+)hr_"
            r"(?P<date>\d{8})_v(?P<version>\d+)\.(?P<ext>\w+)",
            filename.name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with Leinweber-offsets regex."
        )

        if match is None:
            return None

        offset_type_from_folder = filename.parent.name
        if offset_type_from_folder != LEINWEBER:
            raise ValueError(
                f"Offsets file {filename} name implies offset type "
                f"'{LEINWEBER}' but it sits in the '{offset_type_from_folder}' folder."
            )

        return cls(
            sensor=match["sensor"],
            content_date=datetime.strptime(match["date"], "%Y%m%d"),
            duration_hours=int(match["duration"]),
            version=int(match["version"]),
            extension=match["ext"],
        )
