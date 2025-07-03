import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.IFileMetadataProvider import IFileMetadataProvider

logger = logging.getLogger(__name__)


@dataclass
class AncillaryFileMetadataProvider(IFileMetadataProvider):
    """
    Metadata for ancillary files
    """

    mission: str = "imap"
    instrument: str = "mag"
    descriptor: str | None = None
    start_date: datetime | None = None  # start date of validity
    end_date: datetime | None = None  # end date of validity
    extension: str | None = None

    def supports_versioning(self) -> bool:
        return True

    def get_sub_folder(self) -> Path:
        """Get the subfolder for ancillary files."""

        if self.descriptor is None or self.start_date is None:
            logger.error(
                "No 'descriptor' or 'start_date' defined. Cannot determine subfolder."
            )
            raise ValueError(
                "No 'descriptor' or 'start_date' defined. Cannot determine subfolder."
            )

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
                if self.descriptor.endswith("-offsets"):
                    return Path("l2-offsets") / self.start_date.strftime("%Y/%m")
                else:
                    logger.error(
                        f"Unknown descriptor '{self.descriptor}' for ancillary files."
                    )
                    raise ValueError(
                        f"Unknown descriptor '{self.descriptor}' for ancillary files."
                    )

    def get_folder_structure(self) -> str:
        if self.start_date is None:
            logger.error("No 'content_date' defined. Cannot generate folder structure.")
            raise ValueError(
                "No 'content_date' defined. Cannot generate folder structure."
            )

        return (Path("science-ancillary") / self.get_sub_folder()).as_posix()

    def get_filename(self) -> str:
        if (
            self.descriptor is None
            or self.start_date is None
            or self.version is None
            or self.extension is None
        ):
            logger.error(
                "No 'descriptor', 'start_date', 'version', or 'extension' defined. Cannot generate file name."
            )
            raise ValueError(
                "No 'descriptor', 'start_date', 'version', or 'extension' defined. Cannot generate file name."
            )

        if self.end_date is None:
            valid_date_range = self.start_date.strftime("%Y%m%d")
        else:
            valid_date_range = f"{self.start_date.strftime('%Y%m%d')}_{self.end_date.strftime('%Y%m%d')}"

        return f"{self.mission}_{self.instrument}_{self.descriptor}_{valid_date_range}_v{self.version:03}.{self.extension}"

    def get_unversioned_pattern(self) -> re.Pattern:
        if not self.start_date or not self.descriptor or not self.extension:
            logger.error(
                "No 'start_date' or 'descriptor' or 'extension' defined. Cannot generate pattern."
            )
            raise ValueError(
                "No 'start_date' or 'descriptor' or 'extension' defined. Cannot generate pattern."
            )

        if self.end_date is None:
            valid_date_range = self.start_date.strftime("%Y%m%d")
        else:
            valid_date_range = f"{self.start_date.strftime('%Y%m%d')}_{self.end_date.strftime('%Y%m%d')}"

        return re.compile(
            rf"{self.mission}_{self.instrument}_{self.descriptor}_{valid_date_range}_v(?P<version>\d+)\.{self.extension}"
        )

    @classmethod
    def from_filename(
        cls, filename: str | Path
    ) -> "AncillaryFileMetadataProvider | None":
        """Create metadata provider from filename."""

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
