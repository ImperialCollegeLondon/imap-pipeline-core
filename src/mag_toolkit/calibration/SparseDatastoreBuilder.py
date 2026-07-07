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

    What is copied:
      * per-day science + housekeeping for ``[d - days_before, d + days_after]``
        (config ``per_day_patterns``),
      * small day-independent inputs — calibration matrices, profiles, pre-computed
        offsets, spin table, thruster activities (config ``shared_patterns``),
      * the SPICE metakernel and exactly the kernels it references, rewriting the
        metakernel's ``PATH_VALUES`` to the sparse ``spice`` folder so it furnishes
        from the sparse root.
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
        days = self._expanded_days(dates)

        # Ensure there is room in the work folder before copying anything in.
        check_disk_space(target_root.parent, self.disk_usage_threshold)
        target_root.mkdir(parents=True, exist_ok=True)

        copied = 0
        for day in days:
            context = {
                "level": level,
                "mode": mode.value,
                "Y": f"{day.year:04d}",
                "m": f"{day.month:02d}",
                "Ymd": day.strftime("%Y%m%d"),
            }
            for pattern in self.config.per_day_patterns:
                copied += self._copy_glob(pattern.format(**context), target_root)

        shared_context = {"matrix_version": matrix_version}
        for pattern in self.config.shared_patterns:
            copied += self._copy_glob(pattern.format(**shared_context), target_root)

        copied += self._copy_metakernel_and_kernels(metakernel_filename, target_root)

        self._add_case_insensitive_aliases(target_root)

        logger.info(
            f"Built sparse datastore at {target_root} with {copied} files for "
            f"{days[0].date()}..{days[-1].date()} ({mode.value})."
        )
        return target_root

    def _expanded_days(self, dates: list[datetime]) -> list[datetime]:
        all_days: set[datetime] = set()
        for date in dates:
            for offset in range(-self.config.days_before, self.config.days_after + 1):
                day = (date + timedelta(days=offset)).replace(
                    hour=0, minute=0, second=0, microsecond=0, tzinfo=None
                )
                all_days.add(day)
        return sorted(all_days)

    def _add_case_insensitive_aliases(self, target_root: Path) -> None:
        """Add lowercase symlink aliases for folders MATLAB reads case-insensitively.

        The shared datastore is case-insensitive so a single ``Profiles`` folder
        serves both ``Profiles`` and ``profiles`` reads; on the case-sensitive
        sparse copy we add the missing lowercase alias.
        """
        for relative in self.config.case_insensitive_dir_aliases:
            real = target_root / relative
            if not real.exists():
                continue
            lower_name = real.name.lower()
            if lower_name == real.name:
                continue
            alias = real.parent / lower_name
            if not alias.exists():
                alias.symlink_to(real.name)

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
        spice_relative_paths = self._parse_metakernel_kernels(source_mk)
        for kernel_relative in spice_relative_paths:
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

        # Write the metakernel into the sparse spice/mk folder with PATH_VALUES
        # pointing at the sparse spice folder so it furnishes from the sparse root.
        dest_mk = SPICEPathHandler.get_metakernel_path(target_root, metakernel_filename)
        dest_mk.parent.mkdir(parents=True, exist_ok=True)
        dest_mk.write_text(
            self._rewrite_metakernel_path_values(
                source_mk.read_text(), (target_root / "spice").resolve()
            )
        )
        count += 1
        return count

    @staticmethod
    def _parse_metakernel_kernels(metakernel_path: Path) -> list[str]:
        """Return the kernel paths (relative to the datastore ``spice`` folder)
        referenced by a metakernel's ``KERNELS_TO_LOAD`` block."""
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
    def _rewrite_metakernel_path_values(text: str, spice_folder: Path) -> str:
        """Point the metakernel's PATH_VALUES at ``spice_folder`` (absolute) so its
        ``$SYMBOL/...`` kernel entries resolve within the sparse datastore."""
        return re.sub(
            r"PATH_VALUES\s*=\s*\([^)]*\)",
            f"PATH_VALUES     = ( '{spice_folder}' )",
            text,
        )
