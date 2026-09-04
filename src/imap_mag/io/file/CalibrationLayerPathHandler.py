import logging
import re
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import ClassVar

from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler
from imap_mag.util import ScienceMode
from mag_toolkit.calibration.CalibrationDefinitions import (
    CalibrationMethod,
    LayerDataFormat,
)

logger = logging.getLogger(__name__)


@dataclass
class CalibrationLayerPathHandler(VersionedPathHandler):
    """
    Path handler for calibration layers.
    Designed to handle the special internal case of calibration layers that do not obey exact SPDF conventions.
    E.g filemnames like
        imap_mag_noop-norm-layer_20251017_v001.0001.csv
        imap_mag_noop-norm-layer-data_20251017_v001.0001.csv
    """

    mission: str = "imap"
    instrument: str = "mag"
    descriptor: str | None = None
    extra_descriptor: str = ""
    content_date: datetime | None = None  # date data belongs to
    extension: str = "json"
    _has_major_version: bool = True
    original_filename_or_path: str | Path | None = field(compare=False, default=None)

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

        if self._has_major_version:
            return f"{self.mission}_{self.instrument}_{self.descriptor}-layer{self.extra_descriptor}_{self.content_date.strftime('%Y%m%d')}_v{self.version_major:03d}.{self.version:04d}.{self.extension}"
        else:
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
            rf"{self.mission}_{self.instrument}_{full_descriptor}_{self.content_date.strftime('%Y%m%d')}_v(?:(?P<major>\d+)\.)?(?P<version>\d+)\.{self.extension}"
        )

    def is_metadata_file(self) -> bool:
        """Determine if this handler represents a metadata JSON file (true) or a data csv/parquet/cdf type file (false)."""
        return self.extra_descriptor != "-data"

    def _derived(self, **overrides) -> "CalibrationLayerPathHandler":
        """Build a new handler by copying this handler's fields and applying
        overrides. Shared implementation behind every "derive a related handler
        from this one" helper (companion data file, format-conversion output,
        sibling existence checks), so the field list only needs to stay in sync
        with the dataclass in one place.
        """
        return replace(self, original_filename_or_path=None, **overrides)

    def create_new_datafile_handler(
        self, layer_data_format: LayerDataFormat
    ) -> "CalibrationLayerPathHandler":
        """Build a handler for a *new* companion data file in the given format.

        Only for creating new output: the caller must already know what format
        it is writing (csv/parquet). To find the
        companion of an *existing* layer, read its data_filename from a
        CalibrationLayer instance instead (see ``CalibrationLayer.from_file``).
        """
        return self._derived(
            extra_descriptor="-data", extension=layer_data_format.value
        )

    def with_new_primary_format(
        self,
        extension: str,
        versioning_mode: VersionedPathHandler.VersionMode,
        allow_overwrite: bool,
    ) -> "CalibrationLayerPathHandler":
        """Build a handler for this same layer's primary (JSON/CDF) file, but
        targeting a different extension and versioning strategy — used by
        calibrate-convert to derive the output-format handler from the handler
        of the layer being converted.
        """
        return self._derived(
            extension=extension,
            versioning_mode=versioning_mode,
            allow_overwrite=allow_overwrite,
        )

    @classmethod
    def from_filename(
        cls, filename: str | Path
    ) -> "CalibrationLayerPathHandler | None":
        match = re.match(
            r"imap_mag_(?P<descr>[^_]+)?-layer(?P<extra_descr>[^_]+)?_(?P<date>\d{8})_v(?P<major_or_minor>\d+)(?:\.(?P<minor>\d+))?\.(?P<ext>\w+)",
            Path(filename).name,
        )
        logger.debug(
            f"Filename {filename} matches {match.groupdict(0) if match else 'nothing'} with calibration regex."
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
            version_major = 0
            version = int(match["major_or_minor"])
            has_major_version = False

        return cls(
            descriptor=match["descr"],
            extra_descriptor=match["extra_descr"] or "",
            content_date=datetime.strptime(match["date"], "%Y%m%d"),
            version=version,
            version_major=version_major,
            _has_major_version=has_major_version,
            extension=match["ext"],
            original_filename_or_path=filename,
        )

    @classmethod
    def from_method(
        cls,
        method: CalibrationMethod,
        content_date: datetime,
        settings: "AppSettings",  # type: ignore  # noqa: F821
        mode: ScienceMode | None = None,
        version_number_override: tuple[int, int] | None = None,
    ) -> "CalibrationLayerPathHandler":

        major_version = (
            version_number_override[0]
            if version_number_override
            else settings.version_major
        )

        return CalibrationLayerPathHandler(
            descriptor=f"{method.short_name}-{mode.value}"
            if mode is not None
            else method.short_name,
            content_date=content_date,
            version_major=major_version,
            version=version_number_override[1] if version_number_override else 1,
            versioning_mode=VersionedPathHandler.VersionMode.USER_OVERRIDE
            if version_number_override
            else VersionedPathHandler.VersionMode.MAX_VERSION_PLUS_ONE,
        )

    def increase_sequence(self) -> None:
        super().increase_sequence()
        logger.debug(
            f"Increased version to {self.version} for file {self.get_filename()}."
        )

    def _companion_data_path(self, alongside: Path) -> Path:
        """Return the companion data file path, reading data_filename from the
        CalibrationLayer (works for any companion format, csv or parquet).

        Reading the layer's own data_filename field means the lookup stays correct
        regardless of what version number is currently set on the handler — both
        work-folder v001.json and datastore v002.json point at their own companion.
        """
        try:
            from mag_toolkit.calibration.CalibrationLayer import CalibrationLayer

            cal = CalibrationLayer.from_file(alongside, load_contents=False)
            companion = cal.get_datafile_path()
            if companion is not None:
                return companion
        except Exception:
            pass
        # Last resort when the JSON can't be read to determine the real
        # companion extension: guess the default. This path is only used for
        # existence checks below, so a wrong guess just reads as "not found".
        return (
            alongside.parent
            / self.create_new_datafile_handler(LayerDataFormat.PARQUET).get_filename()
        )

    def get_content_identity(
        self, file_path_override: Path | None = None, parent_folder: Path = Path()
    ) -> str:
        """Return a hash representing content identity for deduplication.

        JSON layer files are identified by their companion CSV hash (stored in the
        layer's metadata.data_hash field), not the JSON file itself, because the JSON
        can change (e.g. version bump rewrites data_filename) while the CSV data stays
        the same.
        """
        source_file = (
            file_path_override
            if file_path_override is not None
            else self.get_full_path(parent_folder)
        )

        if not source_file.exists():
            raise FileNotFoundError(
                f"Source file {source_file} does not exist for content identity hashing."
            )

        if self.is_metadata_file():
            try:
                from mag_toolkit.calibration.CalibrationLayer import CalibrationLayer

                cal = CalibrationLayer.from_file(source_file, load_contents=False)
                if cal.metadata.data_hash:
                    return cal.metadata.data_hash
            except Exception:
                pass

            companion = self._companion_data_path(source_file)
            if companion.exists():
                logger.warning(
                    f"No data_hash in metadata of {source_file}. Falling back to hashing the companion CSV."
                )
                return self.default_file_hash(companion)

            logger.debug(
                f"No data_hash or companion CSV for {source_file}. Using JSON file hash as identity fallback."
            )
            # Fall through to hash the JSON itself

        logger.debug(
            f"Content identity for non-JSON file {source_file} is based on the file itself."
        )
        return self.default_file_hash(source_file)

    def prepare_for_version(self, source_file: Path) -> Path:
        """Rewrite the JSON's data_filename to match the handler's current version.

        When the datastore assigns a version other than what the source file was
        originally generated at (e.g., v001 → v002 because v001 already exists
        with different content), the JSON must reference the correctly-versioned
        companion data file. Returns a temporary file that the caller must delete.

        CalibrationLayer owns the actual rewrite logic — it understands the file
        contents best; this is just the polymorphic dispatch point that
        DatastoreFileManager calls on any path handler.

        Only JSON layers have a companion to keep in lock-step; a standalone
        CDF layer (also a "metadata file" per is_metadata_file, but
        self-contained) has no sibling and needs no rewriting.
        """
        if self.extension != "json":
            return source_file

        from mag_toolkit.calibration.CalibrationLayer import CalibrationLayer

        cal_file = CalibrationLayer.from_file(source_file, load_contents=False)
        return cal_file.update_file_contents_based_on_version(self, source_file)

    def is_version_blocked_by_sibling(
        self, version: int, datastore: Path, source_file: Path
    ) -> bool:
        """For JSON layers: also reject a version if the companion data file slot is
        occupied with content that differs from the new companion data file.

        This ensures that JSON and its companion always land on the same version,
        even when only one half of the pair exists in the datastore from a prior
        partial save. A standalone CDF layer has no companion, so is exempt.
        """
        if self.extension != "json":
            return False
        new_companion = self._companion_data_path(source_file)
        # Not creating a new file here, just checking an existing/candidate slot,
        # so build the sibling handler directly with the companion's own extension
        # rather than going through create_new_datafile_handler.
        sibling = self._derived(
            extra_descriptor="-data", extension=new_companion.suffix.lstrip(".")
        )
        sibling.set_sequence(version)
        sibling_dest = sibling.get_full_path(datastore)
        if not sibling_dest.exists():
            return False
        if new_companion.exists():
            return sibling.get_content_identity(
                new_companion
            ) != sibling.get_content_identity(sibling_dest)
        return True  # Cannot determine — play it safe and block this version
