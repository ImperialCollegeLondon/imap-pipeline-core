import logging
import re
import typing
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from imap_mag.io.file.VersionedPathHandler import VersionedPathHandler
from imap_mag.util import TimeConversion

logger = logging.getLogger(__name__)

T = typing.TypeVar("T", bound="SPICEPathHandler")

METAKERNEL_FILENAME_PREFIX = "imap_mag_metakernel"

"""
See https://lasp.colorado.edu/galaxy/spaces/IMAP/pages/221734667/SDC+Expected+SPICE+File+List
for expected SPICE file naming conventions:
de###.bsp
L1_de###.bsp
naif####.tls
pck#####.tpc
earth_000101_yymmdd_yymmdd.bpc
imap_yyyy_doy_yyyy_doy_##.ah.bc and imap_yyyy_doy_yyyy_doy_##.ah.a
imap_yyyy_doy_yyyy_doy_##.ap.bc or imap_yyyy_doy_yyyy_doy_##.ap.a
imap_dps_yyyy_doy_yyyy_doy_v###.ah.bc
imap_yyyy_doy_a##.spice.mk or imap_yyyy_doy_a##.stk_a.mk
imap_yyyy_doy_yyyy_doy_##.spin.csv
imap_yyyy_doy_##.repoint.csv
IMAP_yyyy_doy_e##.mk
imap_launch_yyyymmdd_yyyymmdd_v##.bsp
imap_nom_yyyymmdd_yyyymmdd_v##.bsp
imap_recon_yyyymmdd_yyyymmdd_v##.bsp
imap_pred_yyyymmdd_yyyymmdd_v  ??? bsp?
imap_noburn_yyyymmdd_yyyymmdd_v##.bsp
imap_long_yyyymmdd_yyyymmdd_v##.bsp
imap_yyyy_doy_yyyy_doy_sff_hist__##.sff
imap_###.tf
imap_science_####.tf
imap_sclk_####.tsc
"""


@dataclass
class SPICEPathHandler(VersionedPathHandler):
    """
    Path handler for all SPICE files such as kernels and meta-kernels.

    e.g.
    .work/spice/imap/spice/ck/imap_dps_2025_281_2025_286_001.ah.bc
    .work/spice/imap/spice/sclk/imap_sclk_0003.tsc
    """

    kernel_folder: str | None = None  # e.g., ck, sclk, etc.
    filename: str | None = None
    metadata: dict | None = None
    is_versioned_spice_file: bool = False

    # the content date we try and take from spice file API metadata if possible
    # uses GET /spice-query filed min_date_datetime
    # SPICE files might span long date ranges so this is just the starting date for the content
    content_date: datetime | None = None

    @staticmethod
    def get_root_folder() -> str:
        return "spice"

    matching_file_patterns: typing.ClassVar[list[tuple[str, str]]] = [
        (r"^de.*\.bsp$", "spk"),
        (r"^L1_de.*\.bsp$", "spk"),
        (r"^naif.*\.tls$", "lsk"),
        (r"^pck.*\.tpc$", "pck"),
        (r"^earth_.*\.bpc$", "bpc"),
        (r"^imap_.*\.ah\.bc$", "ck"),
        (r"^imap_.*\.ah\.a$", "ck"),
        (r"^imap_.*\.ap\.bc$", "ck"),
        (r"^imap_.*\.ap\.a$", "ck"),
        (r"^imap_dps_.*\.ah\.bc$", "ck"),
        (r"^imap_.*\.spice\.mk$", "mk"),
        (r"^imap_.*\.stk_a\.mk$", "mk"),
        (r"^imap_.*\.spin\.csv$", "spin"),
        (r"^imap_.*\.repoint\.csv$", "repoint"),
        (r"^IMAP_.*\.mk$", "mk"),
        (r"^imap_launch_.*\.bsp$", "spk"),
        (r"^imap_nom_.*\.bsp$", "spk"),
        (r"^imap_recon_.*\.bsp$", "spk"),
        (r"^imap_pred_.*\.bsp$", "spk"),
        (r"^imap_noburn_.*\.bsp$", "spk"),
        (r"^imap_long_.*\.bsp$", "spk"),
        (r"^imap_.*\.sff$", "activities"),
        (r"^imap_.*\.tf$", "fk"),
        (r"^imap_science_.*\.tf$", "fk"),
        (r"^imap_sclk_.*\.tsc$", "sclk"),
        (rf"^{METAKERNEL_FILENAME_PREFIX}_.*\.tm$", "mk"),
    ]

    def supports_sequencing(self) -> bool:
        return self.is_versioned_spice_file

    def get_unsequenced_pattern(self) -> re.Pattern:
        logger.debug(
            f"Getting unsequenced pattern for SPICE file with filename {self.filename} and version {self.version}. Versioned: {self.is_versioned_spice_file}"
        )

        if not self.is_versioned_spice_file:
            raise ValueError(
                "This SPICE file is not versioned; no unsequenced pattern available."
            )

        super()._check_property_values(
            f"unsequenced pattern for {self.filename}", ["filename", "version"]
        )
        assert self.filename
        assert self.version is not None

        filename_extension = self.filename.split(".")[-1]

        end_of_filename_v = f"_v{self.version:03}.{filename_extension}"
        end_of_filename_no_v = f"_{self.version:03}.{filename_extension}"
        if self.filename.endswith(end_of_filename_v):
            base_filename = self.filename[: -len(end_of_filename_v)]
        elif self.filename.endswith(end_of_filename_no_v):
            base_filename = self.filename[: -len(end_of_filename_no_v)]
        else:
            raise ValueError(
                f"Filename {self.filename} does not end with expected version pattern for versioned SPICE file."
            )

        return re.compile(
            rf"{re.escape(base_filename)}_v(?P<version>\d+)\.{filename_extension}"
        )

    def get_content_date_for_indexing(self) -> datetime | None:
        return self.content_date

    def get_folder_structure(self) -> str:
        super()._check_property_values("folder structure", ["kernel_folder"])
        assert self.kernel_folder

        return (Path(self.get_root_folder()) / self.kernel_folder).as_posix()

    def get_filename(self) -> str:
        super()._check_property_values("get_filename", ["filename"])
        assert self.filename
        return self.filename

    def add_metadata(self, metadata: dict) -> None:
        self.content_date = TimeConversion.try_extract_iso_like_datetime(
            metadata, "min_date_datetime"
        ) or TimeConversion.try_extract_iso_like_datetime(metadata, "ingestion_date")

        self.metadata = metadata

        if metadata.get("version") is not None:
            self.version = int(metadata["version"])

    def get_metadata(self) -> dict | None:
        return self.metadata

    def set_sequence(self, sequence: int) -> None:
        if not self.supports_sequencing():
            raise ValueError("This path handler does not support sequencing.")

        self.version = sequence
        self._regenerate_filename_with_version()

    def increase_sequence(self) -> None:
        if not self.supports_sequencing():
            raise ValueError("This path handler does not support sequencing.")

        self.version += 1

        self._regenerate_filename_with_version()

    def _regenerate_filename_with_version(self):
        if self.filename:
            # Version has been updated, we need to uprev the filename to reflect this
            self.filename = re.sub(
                r"_v\d{3}\.(\w+$)", f"_v{self.version:03}.\\1", self.filename
            )

    @classmethod
    def from_filename(cls: type[T], filename: str | Path) -> T | None:
        if filename is None:
            return None

        is_spice = False
        kernel_type = None
        filename_only = (
            filename.name if isinstance(filename, Path) else Path(filename).name
        )

        # check for Path
        if (
            isinstance(filename, Path)
            and filename.parent.parent.name == SPICEPathHandler.get_root_folder()
        ):
            # if this is in the SPICE folder structure it IS spice and so we just take it as-is
            is_spice = True
            kernel_type = filename.parent.name
        elif isinstance(filename, str):
            filename = Path(filename)

        # See https://lasp.colorado.edu/galaxy/spaces/IMAP/pages/221734667/SDC+Expected+SPICE+File+List
        # for file information
        for file_pattern in cls.matching_file_patterns:
            if re.match(file_pattern[0], filename_only):
                is_spice = True
                # if we did not get the kernel folder from the path then (try) work it out fromt the file name
                kernel_type = file_pattern[1] if kernel_type is None else kernel_type
                break

        if is_spice:
            # file is versioned if it has _v### before the file extension
            versioned_match = re.search(r"_v(\d{3,})\.", filename_only)
            is_versioned_spice_file = versioned_match is not None
            version = int(versioned_match.group(1)) if versioned_match else None

            if kernel_type is None:
                logger.warning(
                    f"Could not determine kernel type for SPICE file {filename}. Setting to 'unknown'."
                )
                kernel_type = "unknown"

            handler = cls(
                kernel_folder=kernel_type,
                filename=filename_only,
            )
            handler.is_versioned_spice_file = is_versioned_spice_file
            if version is not None:
                handler.version = version

            logger.debug(
                f"Created SPICEPathHandler for file {filename} with kernel type {kernel_type} and version {version if is_versioned_spice_file else 'N/A'}."
            )
            return handler

        return None
