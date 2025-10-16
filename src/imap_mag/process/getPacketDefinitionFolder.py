import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def getPacketDefinitionFolder(packet_definition: Path) -> Path:
    """Retrieve path of packet definition folder, based on provided relative path."""

    paths_to_try: dict[str, Path] = {
        "relative": packet_definition,
        "module": Path(os.path.dirname(__file__)).parent / packet_definition,
    }

    paths_to_try_string: str = "\n".join(
        [f"    {source}: {path}" for source, path in paths_to_try.items()]
    )
    logger.debug(
        f"Trying XTCE packet definition folder from these paths in turn:\n{paths_to_try_string}"
    )

    for source, path in paths_to_try.items():
        if path and path.exists():
            logger.debug(
                f"Using XTCE packet definition folder from {source} path: {path}"
            )
            return path
    else:
        raise FileNotFoundError(
            f"XTCE packet definition folder not found: {packet_definition}"
        )
