from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.file.IFilePathHandler import IFilePathHandler


@dataclass
class LatestFilePathHandler(IFilePathHandler):
    """
    Path handler for "latest" files.
    """

    root: Path | None = None
    extension: str | None = None
    latest_date: datetime | None = None

    def supports_sequencing(self) -> bool:
        return False

    def get_content_date_for_indexing(self) -> datetime | None:
        return self.latest_date

    def get_folder_structure(self) -> str:
        self._check_property_values("folder structure", ["root"])
        assert self.root

        return self.root.as_posix()

    def get_filename(self) -> str:
        self._check_property_values("file name", ["extension"])
        assert self.extension

        return f"latest.{self.extension}"

    def add_metadata(self, metadata: dict) -> None:
        raise NotImplementedError()

    def get_metadata(self) -> dict | None:
        return None

    @classmethod
    def from_filename(cls, filename: str | Path) -> "LatestFilePathHandler | None":
        raise NotImplementedError(
            "from_filename is not implemented for LatestFilePathHandler."
        )
