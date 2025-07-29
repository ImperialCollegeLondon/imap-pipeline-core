import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_data_access.file_validation import _SPICE_DIR_MAPPING, SPICEFilePath

from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler

logger = logging.getLogger(__name__)


@dataclass
class SpicePathHandler(VersionedPathHandler):
    """
    Path handler for SPICE files.
    """

    root_folder: str = "spice"
    mission: str = "imap"
    type: str | None = None
    start_date: datetime | None = None  # start date of validity
    end_date: datetime | None = None  # end date of validity
    ingestion_date: datetime | None = None  # date data was ingested by SDC
    extension: str | None = None

    filename: str | None = None

    def __init__(self, *args, **kwargs):
        raise NotImplementedError(
            "SPICEPathHandler cannot not be instantiated directly. Use from_filename instead."
        )

    def get_folder_structure(self) -> str:
        if not self.type or (self.type not in _SPICE_DIR_MAPPING):
            logger.error("No valid 'type' defined. Cannot generate folder structure.")
            raise ValueError(
                "No valid 'type' defined. Cannot generate folder structure."
            )

        if self.start_date:
            date: str = self.start_date.strftime("%Y/%m")
        else:
            date = ""

        return (
            Path(self.root_folder) / _SPICE_DIR_MAPPING[self.type] / date
        ).as_posix()

    def get_filename(self) -> str:
        super()._check_property_values("file name", ["filename"])
        assert self.filename

        return self.filename

    def get_unsequenced_pattern(self) -> re.Pattern:
        filename: str = self.get_filename()
        return re.compile(re.sub(r"v\d{3}", "v(?P<version>\\d+)", filename))

    @classmethod
    def from_filename(cls, filename: str | Path) -> "SpicePathHandler | None":
        filename = Path(filename)

        try:
            metadata: dict | None = SPICEFilePath.extract_filename_components(filename)
        except SPICEFilePath.InvalidImapFileError:
            return None

        if not metadata:
            return None
        else:
            # SPICE file names are too complicated, so store the original filename
            # and disallow instantiation of the class directly.
            return cls(
                type=metadata["type"],
                start_date=datetime.strptime(metadata["start_date"], "%Y%m%d")
                if "start_date" in metadata
                else None,
                end_date=datetime.strptime(metadata["end_date"], "%Y%m%d")
                if "end_date" in metadata
                else None,
                version=int(metadata["version"]),
                extension=metadata["extension"],
                filename=filename.name,
            )
