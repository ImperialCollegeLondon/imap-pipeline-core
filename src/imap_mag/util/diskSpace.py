import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


def check_disk_space(path: Path, threshold: float) -> None:
    """Raise OSError if the filesystem containing path meets or exceeds the usage threshold."""
    check_path = path
    while not check_path.exists() and check_path != check_path.parent:
        check_path = check_path.parent
    if not check_path.exists():
        return

    usage = shutil.disk_usage(check_path)
    used_fraction = usage.used / usage.total
    if used_fraction >= threshold:
        raise OSError(
            f"Disk usage at {path} is {used_fraction:.1%}, which meets or exceeds the "
            f"{threshold:.1%} threshold. File operations are blocked to protect storage."
        )
