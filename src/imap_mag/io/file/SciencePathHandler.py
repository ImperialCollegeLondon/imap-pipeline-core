import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.file.StandardSPDFPathHandler import StandardSPDFPathHandler
from imap_mag.util import MAGSensor, ScienceMode

logger = logging.getLogger(__name__)


@dataclass
class SciencePathHandler(StandardSPDFPathHandler):
    """
    Path handler for science files.
    """

    root_folder: str = "science"
    ingestion_date: datetime | None = None  # date data was ingested by SDC

    version_is_locked: bool = False

    def get_mode(self) -> ScienceMode:
        return (
            ScienceMode.Burst
            if self.descriptor and ScienceMode.Burst.short_name in self.descriptor
            else ScienceMode.Normal
        )

    def get_sensor(self) -> MAGSensor:
        return (
            MAGSensor.IBS
            if self.descriptor and MAGSensor.IBS.value in self.descriptor
            else MAGSensor.OBS
        )

    def get_folder_structure(self) -> str:
        super()._check_property_values("folder structure", ["content_date", "level"])
        assert self.content_date and self.level

        return (
            Path(self.root_folder)
            / self.instrument
            / self.level
            / self.content_date.strftime("%Y/%m")
        ).as_posix()

    def set_sequence(self, sequence: int) -> None:
        if self.version_is_locked and sequence != self.version:
            raise ValueError(
                "This science file version is locked and cannot be changed."
            )

        super().set_sequence(sequence)

    def increase_sequence(self) -> None:
        if self.version_is_locked:
            raise ValueError(
                "This science file version is locked and cannot be changed."
            )

        super().increase_sequence()

    @classmethod
    def from_filename(
        cls, filename: str | Path, version_is_locked: bool = False
    ) -> "SciencePathHandler | None":
        match = re.match(
            r"imap_mag_(?P<level>l\d[a-zA-Z]?(?:-pre)?)_(?P<descr>(?:norm|burst)[^_]*)_(?P<date>\d{8})_v(?P<major_or_minor>\d+)(?:\.(?P<minor>\d+))?\.(?P<ext>\w+)",
            Path(filename).name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with science regex."
        )

        if match is None:
            return None

        if match["minor"] is not None:
            # New format: _vMMM.mmmm.ext
            version_major = int(match["major_or_minor"])
            version = int(match["minor"])
            has_major_version = True
        else:
            # Legacy format: _vNNN.ext
            version_major = 1
            version = int(match["major_or_minor"])
            has_major_version = False

        return cls(
            level=match["level"],
            descriptor=match["descr"],
            content_date=datetime.strptime(match["date"], "%Y%m%d"),
            version=version,
            version_major=version_major,
            has_major_version=has_major_version,
            extension=match["ext"],
            version_is_locked=version_is_locked,
        )
