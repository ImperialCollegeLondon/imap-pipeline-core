import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.file.StandardSPDFPathHandler import StandardSPDFPathHandler
from imap_mag.util import MAGSensor, ScienceMode
from imap_mag.util.DataVersion import get_cdf_data_version
from mag_toolkit.calibration.CalibrationDefinitions import CONSTANTS

logger = logging.getLogger(__name__)


@dataclass
class SciencePathHandler(StandardSPDFPathHandler):
    """
    Path handler for science files.
    """

    root_folder: str = "science"
    ingestion_date: datetime | None = None  # date data was ingested by SDC

    version_is_locked: bool = False

    extension: str | None = "cdf"

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

    def can_change_sequence(self) -> bool:
        return not self.version_is_locked and super().can_change_sequence()

    def increase_sequence(self) -> None:
        if self.version_is_locked:
            raise ValueError(
                "This science file version is locked and cannot be changed."
            )

        super().increase_sequence()

    def prepare_for_version(self, source_file: Path) -> Path:
        """Stamp an L2-pre CDF with the version it is being saved under."""
        if self.level != "l2-pre" or self.get_extension_or_default() != "cdf":
            return source_file

        current = self.from_filename(source_file)
        if current is not None and current.get_filename() == self.get_filename():
            # no change in version
            return source_file

        logger.info(
            f"Updating version attributes in {source_file} for {self.get_filename()}"
        )
        from spacepy import pycdf

        with pycdf.CDF(str(source_file), readonly=False) as cdf:
            cdf.attrs[CONSTANTS.CDF_ATTRS.DATA_VERSION] = get_cdf_data_version(
                self.version,
                self.version_major if self.has_major_version else 0,
            )
            cdf.attrs[CONSTANTS.CDF_ATTRS.LOGICAL_FILE_ID] = Path(
                self.get_filename()
            ).stem
        return source_file

    @classmethod
    def from_filename(
        cls, filename: str | Path, version_is_locked: bool = False
    ) -> "SciencePathHandler | None":
        match = re.match(
            r"imap_mag_(?P<level>l\d[a-zA-Z]?(?:-pre)?)_(?P<descr>(?:norm|burst)[^_]*)_(?P<date>\d{8})_v(?P<major_or_minor>\d+)(?:\.(?P<minor>\d+))?\.?(?P<ext>[A-Za-z][A-Za-z0-9]*)?$",
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
            extension=match["ext"] if match["ext"] is not None else cls.extension,
            version_is_locked=version_is_locked,
        )
