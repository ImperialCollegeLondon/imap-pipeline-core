import logging
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from imap_mag.config.CalibrationCommandConfig import SparseDatastoreConfig
from imap_mag.io.file import SPICEPathHandler
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
        disk_usage_threshold: float = 0.95,
    ):
        self.source_datastore = Path(source_datastore)
        self.config = config
        self.disk_usage_threshold = disk_usage_threshold

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

        copied = 0
        for pattern in self.config.patterns:
            # {level}/{mode}/{matrix_version} are filled first; strftime fills the
            # date codes per day in the pattern's window.
            named = pattern.pattern.format(
                level=level, mode=mode.value, matrix_version=matrix_version
            )
            for day in self._pattern_days(
                dates, pattern.days_before, pattern.days_after
            ):
                copied += self._copy_glob(day.strftime(named), target_root)

        copied += self._copy_metakernel_and_kernels(metakernel_filename, target_root)

        logger.info(
            f"Built sparse datastore at {target_root} with {copied} files "
            f"for {[d.date() for d in dates]} ({mode.value})."
        )
        return target_root

    @staticmethod
    def _pattern_days(
        dates: list[datetime], days_before: int, days_after: int
    ) -> list[datetime]:
        all_days: set[datetime] = set()
        for date in dates:
            for offset in range(-days_before, days_after + 1):
                all_days.add(
                    (date + timedelta(days=offset)).replace(
                        hour=0, minute=0, second=0, microsecond=0, tzinfo=None
                    )
                )
        return sorted(all_days)

    def _copy_glob(self, relative_pattern: str, target_root: Path) -> int:
        count = 0
        for source in self.source_datastore.glob(relative_pattern):
            if source.is_file():
                relative = source.relative_to(self.source_datastore)
                count += self._copy_file(source, target_root / relative)
        return count

    @staticmethod
    def _copy_file(source: Path, destination: Path) -> int:
        if destination.exists():
            return 0
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        return 1

    def _copy_metakernel_and_kernels(
        self, metakernel_filename: str, target_root: Path
    ) -> int:
        source_mk = SPICEPathHandler.get_metakernel_path(
            self.source_datastore, metakernel_filename
        )
        if not source_mk.exists():
            raise FileNotFoundError(
                f"Metakernel {source_mk} not found while building sparse datastore."
            )

        count = 0
        for kernel_relative in self._parse_metakernel_kernels(source_mk):
            source_kernel = self.source_datastore / "spice" / kernel_relative
            if source_kernel.exists():
                count += self._copy_file(
                    source_kernel, target_root / "spice" / kernel_relative
                )
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
        dest_mk.write_text(self._rewrite_metakernel_path_values(source_mk.read_text()))
        count += 1
        return count

    @staticmethod
    def _parse_metakernel_kernels(metakernel_path: Path) -> list[str]:
        """Return the kernel paths (relative to the datastore ``spice`` folder)
        referenced by a metakernel's ``KERNELS_TO_LOAD`` block.

        Assumes the metakernel's kernel entries are relative to the datastore's
        ``spice`` folder (i.e. ``PATH_VALUES`` is ``spice``), which is how the
        production metakernels are written.
        """
        text = metakernel_path.read_text()
        block_match = re.search(
            r"KERNELS_TO_LOAD\s*=\s*\((?P<body>.*?)\)", text, re.DOTALL
        )
        if not block_match:
            return []
        entries = re.findall(r"'([^']*)'", block_match.group("body"))
        # Strip the leading "$SYMBOL/" so each entry is relative to the spice
        # folder (e.g. "$KERNELS/lsk/naif0012.tls" -> "lsk/naif0012.tls").
        return [re.sub(r"^\$\w+/", "", entry).lstrip("/") for entry in entries]

    @staticmethod
    def _rewrite_metakernel_path_values(text: str) -> str:
        """Normalise the metakernel's PATH_VALUES to the relative ``spice`` folder."""
        return re.sub(
            r"PATH_VALUES\s*=\s*\([^)]*\)",
            "PATH_VALUES     = ( 'spice' )",
            text,
        )
