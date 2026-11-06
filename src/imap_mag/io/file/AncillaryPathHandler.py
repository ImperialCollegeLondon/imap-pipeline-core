import logging
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler
from imap_mag.util.DataVersion import get_cdf_data_version, get_gse_version_string
from mag_toolkit.calibration.CalibrationDefinitions import CONSTANTS

logger = logging.getLogger(__name__)


@dataclass
class AncillaryPathHandler(VersionedPathHandler):
    """
    Path handler for ancillary files
    """

    mission: str = "imap"
    instrument: str = "mag"
    descriptor: str | None = None
    start_date: datetime | None = None  # start date of validity
    end_date: datetime | None = None  # end date of validity
    extension: str | None = None

    def get_content_date_for_indexing(self) -> datetime | None:
        """Get the current date."""
        return self.start_date if self._is_offsets_file() else None

    def _is_offsets_file(self) -> bool:
        """Check if the file is an offsets file."""
        return self.descriptor is not None and self.descriptor.endswith("-offsets")

    def get_sub_folder(self) -> Path:
        """Get the subfolder for ancillary files."""

        super()._check_property_values("subfolder", ["descriptor", "start_date"])
        assert self.descriptor and self.start_date

        match self.descriptor:
            case "ialirt-calibration":
                return Path("ialirt")
            case "l1d-calibration":
                return Path("l1d")
            case "l2-calibration":
                return Path("l2-rotation")
            case "l1b-calibration":
                return Path("l1b")
            case _:
                if self._is_offsets_file():
                    return Path("l2-offsets") / self.start_date.strftime("%Y/%m")
                else:
                    logger.error(
                        f"Unknown descriptor '{self.descriptor}' for ancillary files."
                    )
                    raise ValueError(
                        f"Unknown descriptor '{self.descriptor}' for ancillary files."
                    )

    def get_folder_structure(self) -> str:
        super()._check_property_values("folder structure", ["start_date"])
        assert self.start_date

        return (Path("science-ancillary") / self.get_sub_folder()).as_posix()

    def get_filename(self) -> str:
        super()._check_property_values(
            "file name", ["descriptor", "start_date", "extension"]
        )
        assert self.start_date

        if self.end_date is None:
            valid_date_range = self.start_date.strftime("%Y%m%d")
        else:
            valid_date_range = f"{self.start_date.strftime('%Y%m%d')}_{self.end_date.strftime('%Y%m%d')}"

        return f"{self.mission}_{self.instrument}_{self.descriptor}_{valid_date_range}_v{self.version:03}.{self.extension}"

    def prepare_for_version(self, source_file: Path) -> Path:
        """Stamp an offsets CDF with the version and name it is being saved under.

        The offsets file is written before the datastore assigns its final version,
        so the CDF ``Data_version`` and ``Logical_file_id`` attributes are rewritten
        here on a temporary copy. Returns the copy, which the caller must delete;
        other ancillary files are returned unchanged.
        """
        if not self._is_offsets_file() or self.extension != "cdf":
            return source_file

        current = self.from_filename(source_file)
        assert current is not None
        if (
            current.version_major == self.version_major
            and current.version == self.version
        ):
            # no change in version
            return source_file

        from spacepy import (
            pycdf,  # imported locally, see tests.util.miscellaneous.open_cdf
        )

        new_version_path = source_file.parent / self.get_filename()

        logger.info(
            f"Copying {source_file} to {new_version_path} then setting version attributes"
        )
        shutil.copy2(source_file, new_version_path)

        try:
            with pycdf.CDF(str(new_version_path), readonly=False) as cdf:
                cdf.attrs[CONSTANTS.CDF_ATTRS.DATA_VERSION] = get_cdf_data_version(
                    self.version, self.version_major
                )
                cdf.attrs[CONSTANTS.CDF_ATTRS.LOGICAL_FILE_ID] = Path(
                    new_version_path.name
                ).stem
                cdf.attrs[CONSTANTS.CDF_ATTRS.GROUND_SOFTWARE_VERSION_MAG] = (
                    get_gse_version_string()
                )
        except Exception:
            new_version_path.unlink(missing_ok=True)
            raise
        return new_version_path

    def get_unsequenced_pattern(self) -> re.Pattern:
        super()._check_property_values(
            "pattern", ["descriptor", "start_date", "extension"]
        )
        assert self.descriptor and self.start_date

        if self.end_date is None:
            valid_date_range = self.start_date.strftime("%Y%m%d")
        else:
            valid_date_range = f"{self.start_date.strftime('%Y%m%d')}_{self.end_date.strftime('%Y%m%d')}"

        return re.compile(
            rf"{self.mission}_{self.instrument}_{re.escape(self.descriptor)}_{valid_date_range}_v(?P<version>\d+)\.{self.extension}"
        )

    @classmethod
    def from_filename(cls, filename: str | Path) -> "AncillaryPathHandler | None":
        match = re.match(
            r"imap_mag_(?P<descr>[^_]+(-calibration|-offsets))_(?P<start>\d{8})_((?P<end>\d{8})_)?v(?P<version>\d+)\.(?P<ext>\w+)",
            Path(filename).name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with ancillary file regex."
        )

        if match is None:
            return None
        else:
            return cls(
                descriptor=match["descr"],
                start_date=datetime.strptime(match["start"], "%Y%m%d"),
                end_date=datetime.strptime(match["end"], "%Y%m%d")
                if match["end"]
                else None,
                version=int(match["version"]),
                extension=match["ext"],
            )
