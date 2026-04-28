import hashlib
import json
import logging
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar

from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler

logger = logging.getLogger(__name__)


@dataclass
class CalibrationLayerPathHandler(VersionedPathHandler):
    """
    Path handler for calibration layers.
    Designed to handle the special internal case of calibration layers that do not obey exact SPDF conventions.
    E.g filemnames like
        imap_mag_noop-norm-layer_20251017_v001.csv
        imap_mag_noop-norm-layer-data_20251017_v001.csv
    """

    mission: str = "imap"
    instrument: str = "mag"
    descriptor: str | None = None
    extra_descriptor: str = ""
    content_date: datetime | None = None  # date data belongs to
    extension: str = "json"

    DESCRIPTOR_WILDCARD: ClassVar[str] = "*"

    def get_folder_structure(self) -> str:
        super()._check_property_values("folder structure", ["content_date"])
        assert self.content_date

        return (
            Path("calibration") / "layers" / self.content_date.strftime("%Y/%m")
        ).as_posix()

    def get_content_date_for_indexing(self):
        return self.content_date

    def get_filename(self) -> str:
        super()._check_property_values("file name", ["descriptor", "content_date"])
        assert self.content_date

        return f"{self.mission}_{self.instrument}_{self.descriptor}-layer{self.extra_descriptor}_{self.content_date.strftime('%Y%m%d')}_v{self.version:03d}.{self.extension}"

    def get_unsequenced_pattern(self) -> re.Pattern:
        super()._check_property_values("pattern", ["descriptor", "content_date"])
        assert self.descriptor and self.content_date

        if self.descriptor == CalibrationLayerPathHandler.DESCRIPTOR_WILDCARD:
            full_descriptor = rf".+-layer{re.escape(self.extra_descriptor)}"
        else:
            full_descriptor = (
                f"{re.escape(self.descriptor)}-layer{re.escape(self.extra_descriptor)}"
            )

        return re.compile(
            rf"{self.mission}_{self.instrument}_{full_descriptor}_{self.content_date.strftime('%Y%m%d')}_v(?P<version>\d+)\.{self.extension}"
        )

    def get_equivalent_data_handler(self) -> "CalibrationLayerPathHandler":
        return CalibrationLayerPathHandler(
            descriptor=self.descriptor,
            extra_descriptor="-data",
            content_date=self.content_date,
            version=self.version,
            extension="csv",
        )

    @classmethod
    def from_filename(
        cls, filename: str | Path
    ) -> "CalibrationLayerPathHandler | None":
        match = re.match(
            r"imap_mag_(?P<descr>[^_]+)?-layer(?P<extra_descr>[^_]+)?_(?P<date>\d{8})_v(?P<version>\d+)\.(?P<ext>\w+)",
            Path(filename).name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with calibration regex."
        )

        if match is None:
            return None
        else:
            return cls(
                descriptor=match["descr"],
                extra_descriptor=match["extra_descr"] or "",
                content_date=datetime.strptime(match["date"], "%Y%m%d"),
                version=int(match["version"]),
                extension=match["ext"],
            )

    def increase_sequence(self) -> None:
        super().increase_sequence()
        logger.debug(
            f"Increased version to {self.version} for file {self.get_filename()}."
        )

    # ── Content-identity overrides for JSON metadata files ──────────────────

    def _companion_csv_path(self, alongside: Path) -> Path:
        """Return the companion CSV path, reading data_filename from the JSON when possible.

        Reading the JSON's own data_filename field means the lookup stays correct
        regardless of what version number is currently set on the handler — both
        work-folder v001.json and datastore v002.json point at their own companion.
        """
        try:
            layer_dict = json.loads(alongside.read_text())
            data_filename = layer_dict.get("metadata", {}).get("data_filename", "")
            if data_filename:
                return alongside.parent / Path(data_filename).name
        except Exception:
            pass
        return alongside.parent / self.get_equivalent_data_handler().get_filename()

    def get_content_identity(self, source_file: Path) -> str:
        """For JSON metadata files use the companion CSV hash as identity.

        Two calibration layers are identical when their data CSV content is the
        same for the same content date, regardless of which version number the
        JSON happens to embed in its data_filename field.
        """
        if self.extension == "json":
            companion = self._companion_csv_path(source_file)
            if companion.exists():
                return hashlib.md5(companion.read_bytes()).hexdigest()
        return hashlib.md5(source_file.read_bytes()).hexdigest()

    def get_stored_content_identity(self, file_record: Any) -> str:
        """For JSON records read the companion CSV hash stored in file_meta."""
        if self.extension == "json" and file_record.file_meta:
            return file_record.file_meta.get("data_file_hash", file_record.hash)
        return file_record.hash

    def prepare_for_version(self, source_file: Path) -> Path:
        """Rewrite the JSON's data_filename to match the handler's current version.

        When the datastore assigns a version other than what the source file was
        originally generated at (e.g., v001 → v002 because v001 already exists
        with different content), the JSON must reference the correctly-versioned
        companion CSV.  Returns a temporary file that the caller must delete.
        """
        if self.extension != "json":
            return source_file

        expected_data_filename = self.get_equivalent_data_handler().get_filename()
        layer_dict = json.loads(source_file.read_text())
        current_data_filename = Path(
            layer_dict.get("metadata", {}).get("data_filename", "")
        ).name

        if current_data_filename == expected_data_filename:
            return source_file  # already correct — no rewrite needed

        layer_dict["metadata"]["data_filename"] = expected_data_filename
        tmp = Path(tempfile.mktemp(suffix=".json", dir=source_file.parent))
        tmp.write_text(json.dumps(layer_dict))
        logger.debug(
            f"Rewrote {source_file.name} data_filename from {current_data_filename!r} to {expected_data_filename!r} in {tmp.name}."
        )
        return tmp

    def is_version_blocked_by_sibling(
        self, version: int, datastore: Path, source_file: Path
    ) -> bool:
        """For JSON layers: also reject a version if the companion CSV slot is occupied
        with content that differs from the new companion CSV.

        This ensures that JSON and CSV always land on the same version, even when
        only one half of the pair exists in the datastore from a prior partial save.
        """
        if self.extension != "json":
            return False
        sibling = self.get_equivalent_data_handler()
        sibling.set_sequence(version)
        sibling_dest = sibling.get_full_path(datastore)
        if not sibling_dest.exists():
            return False
        new_companion = self._companion_csv_path(source_file)
        if new_companion.exists():
            return sibling.get_content_identity(
                new_companion
            ) != sibling.get_content_identity(sibling_dest)
        return True  # Cannot determine — play it safe and block this version

    def get_storage_meta(self, source_file: Path) -> dict | None:
        """Store the companion CSV hash so duplicate detection works via the DB."""
        if self.extension == "json":
            companion = self._companion_csv_path(source_file)
            if companion.exists():
                return {
                    "data_file_hash": hashlib.md5(companion.read_bytes()).hexdigest()
                }
        return None
