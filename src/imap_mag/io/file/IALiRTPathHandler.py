import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.file.IFilePathHandler import IFilePathHandler

logger = logging.getLogger(__name__)


@dataclass
class IALiRTPathHandler(IFilePathHandler):
    """
    Path handler for I-ALiRT files.
    """

    root_folder: str = "ialirt"

    mission: str = "imap"
    instrument: str = "mag"
    content_date: datetime | None = None  # date data belongs to
    extension: str = "csv"
    is_hk: bool = False
    is_legacy: bool = False  # if legacy naming

    def supports_sequencing(self) -> bool:
        return False

    def get_content_date_for_indexing(self) -> datetime | None:
        return self.content_date

    def get_folder_structure(self) -> str:
        super()._check_property_values("folder structure", ["content_date"])
        assert self.content_date

        # Update root if it's housekeeping data
        self.root_folder = "ialirt_hk" if self.is_hk else self.root_folder

        return (Path(self.root_folder) / self.content_date.strftime("%Y/%m")).as_posix()

    def get_filename(self) -> str:
        super()._check_property_values("file name", ["content_date"])
        assert self.content_date

        date_str = self.content_date.strftime("%Y%m%d")

        hk_suffix = "_hk" if self.is_hk else ""

        return f"{self.mission}_ialirt_{self.instrument.lower()}{hk_suffix}_{date_str}.{self.extension}"

    def add_metadata(self, metadata: dict) -> None:
        raise NotImplementedError()

    def get_metadata(self) -> dict | None:
        return None

    @classmethod
    def is_legacy_name(cls, filename: str | Path):
        match = re.match(
            r"imap_ialirt_(?:(?P<instrument>\w+?)(?P<hk>_hk)?_)?(?P<date>\d{8})\.(?P<ext>\w+)",
            Path(filename).name,
        )

        is_legacy = False
        if match:
            groups = match.groupdict()
            is_legacy = groups["instrument"] is None

        return is_legacy

    @classmethod
    def from_filename(cls, filename: str | Path) -> "IALiRTPathHandler | None":

        # Notice the (?: ... )? wrapping the instrument and _hk parts.
        # This makes the entire instrument block AND its trailing underscore optional!
        match = re.match(
            r"imap_ialirt_(?:(?P<instrument>\w+?)(?P<hk>_hk)?_)?(?P<date>\d{8})\.(?P<ext>\w+)",
            Path(filename).name,
        )

        if not match:
            logger.debug(
                f"Could not parse filename {filename} with IALiRTPathHandler. Skipping."
            )
            return None

        groups = match.groupdict()

        base_instrument = groups["instrument"] or "mag"

        final_instrument = f"{base_instrument}_hk" if groups["hk"] else base_instrument

        logger.debug(
            f"Filename {filename} parsed successfully: instrument={final_instrument}, date={groups['date']}"
        )

        return cls(
            instrument=final_instrument,
            content_date=datetime.strptime(groups["date"], "%Y%m%d"),
            extension=groups["ext"],
            is_hk=bool(groups["hk"]),  # If "_hk" was found in the regex
        )
