import logging
import shutil
from datetime import datetime
from pathlib import Path

from imap_mag.config.CalibrationCommandConfig import SparseDatastoreConfig
from imap_mag.io.file import SPICEPathHandler
from imap_mag.io.FileFinder import FileFinder
from imap_mag.util import ScienceMode
from imap_mag.util.diskSpace import check_disk_space

logger = logging.getLogger(__name__)


class SparseDatastoreBuilder:
    """Builds a sparse (partial) copy of the datastore in the work folder.

    Only the files the MATLAB L2 calibration needs for the days being calibrated
    are copied, preserving their datastore-relative layout so the MATLAB script
    (and SPICE) resolve them from the sparse root. This keeps calibration off the
    (potentially huge, network-mounted) shared datastore for the actual run.

    Which files are copied is driven entirely by the configured glob patterns
    (:class:`SparseDatastoreConfig`), each with its own optional day window. The
    SPICE metakernel and exactly the kernels it references are always copied
    separately, with the metakernel's ``PATH_VALUES`` normalised to the relative
    ``spice`` folder so it furnishes from the sparse root.
    """

    def __init__(
        self,
        source_datastore: Path,
        config: SparseDatastoreConfig,
        disk_usage_threshold: float,
    ):
        """Args:
        source_datastore: Root of the datastore to copy from.
        config: Patterns (and their day windows) to copy.
        disk_usage_threshold: Fraction of disk usage above which copying is
            blocked; must come from ``AppSettings.disk_usage_threshold`` so it is
            configurable, not a code default.
        """
        self.source_datastore = Path(source_datastore)
        self.config = config
        self.disk_usage_threshold = disk_usage_threshold
        self._finder = FileFinder(self.source_datastore)

    def build(
        self,
        target_root: Path,
        dates: list[datetime],
        mode: ScienceMode,
        metakernel_filename: str,
        matrix_version: int | None = None,
    ) -> Path:
        """Populate ``target_root`` with a sparse datastore and return it."""
        level = "l1b" if mode == ScienceMode.Burst else "l1c"

        # Ensure there is room in the work folder before copying anything in.
        check_disk_space(target_root.parent, self.disk_usage_threshold)
        target_root.mkdir(parents=True, exist_ok=True)

        search_start = min(dates)
        search_end = max(dates)

        copied_files = 0
        copied_bytes = 0
        for pattern in self.config.patterns:
            # {level}/{mode}/{matrix_version} are filled first, leaving any
            # {from_doy}/{to_doy}/{sequence} placeholders for the FileFinder; dated
            # patterns then have their strftime date codes filled in per day.
            named = self._substitute_placeholders(
                pattern.pattern, level, mode, matrix_version
            )

            matches = self._finder.find_matching_files(
                named,
                start_date=search_start,
                end_date=search_end,
                days_before=pattern.days_before,
                days_after=pattern.days_after,
                highest_sequence_only=pattern.highest_sequence_only,
                get_previous_if_empty=pattern.get_previous_if_empty,
            )

            for source in matches:
                relative = source.relative_to(self.source_datastore)
                size = self._copy_file(source, target_root / relative)
                if size:
                    copied_files += 1
                    copied_bytes += size

        metakernel_files, metakernel_bytes = self._copy_metakernel_and_kernels(
            metakernel_filename, target_root
        )
        copied_files += metakernel_files
        copied_bytes += metakernel_bytes

        logger.info(
            f"Built sparse datastore at {target_root} with {copied_files} files "
            f"({copied_bytes / (1024**2):.1f} MB) for {[d.date() for d in dates]} "
            f"({mode.value})."
        )
        return target_root

    @staticmethod
    def _substitute_placeholders(
        pattern: str, level: str, mode: ScienceMode, matrix_version: int | None
    ) -> str:
        """Fill in ``{level}``/``{mode}``/``{matrix_version}``, leaving any other
        placeholders (``{from_doy}``, ``{to_doy}``, ``{sequence}``) untouched for
        the FileFinder to resolve."""
        return (
            pattern.replace("{level}", level)
            .replace("{mode}", mode.value)
            .replace("{matrix_version}", str(matrix_version))
        )

    def _copy_file(self, source: Path, destination: Path) -> int:
        """Copy ``source`` to ``destination`` if not already there, logging the
        file and its size. Returns the number of bytes copied (0 if skipped)."""
        if destination.exists():
            return 0

        check_disk_space(destination.parent, self.disk_usage_threshold)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)

        size = destination.stat().st_size
        logger.debug(f"Copied {source} ({size:,} bytes) -> {destination}")
        return size

    def _copy_metakernel_and_kernels(
        self, metakernel_filename: str, target_root: Path
    ) -> tuple[int, int]:
        source_mk = SPICEPathHandler.get_metakernel_path(
            self.source_datastore, metakernel_filename
        )
        if not source_mk.exists():
            raise FileNotFoundError(
                f"Metakernel {source_mk} not found while building sparse datastore."
            )

        files = 0
        total_bytes = 0
        for kernel_relative in SPICEPathHandler.parse_metakernel_kernels(source_mk):
            source_kernel = self.source_datastore / "spice" / kernel_relative
            if source_kernel.exists():
                size = self._copy_file(
                    source_kernel, target_root / "spice" / kernel_relative
                )
                if size:
                    files += 1
                    total_bytes += size
            else:
                logger.warning(
                    f"Kernel '{kernel_relative}' referenced by {metakernel_filename} "
                    f"not found at {source_kernel}; skipping."
                )

        # Write the metakernel into the sparse spice/mk folder with a relative
        # PATH_VALUES so it furnishes from the sparse root (MATLAB cd's there via
        # spice_metakernal_root before furnishing). A relative value also avoids
        # SPICE's limit on the length of a metakernel path token.
        dest_mk = SPICEPathHandler.get_metakernel_path(target_root, metakernel_filename)
        dest_mk.parent.mkdir(parents=True, exist_ok=True)
        rewritten = SPICEPathHandler.rewrite_metakernel_path_values(
            source_mk.read_text()
        )
        dest_mk.write_text(rewritten)
        files += 1
        total_bytes += len(rewritten.encode())
        return files, total_bytes
