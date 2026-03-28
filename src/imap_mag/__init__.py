"""The main module for project."""

import logging
from importlib.metadata import PackageNotFoundError, version

logging.getLogger("imap_mag").setLevel(logging.INFO)
logging.getLogger("imap_processing").setLevel(logging.INFO)
logging.getLogger("imap_data_access").setLevel(logging.INFO)
logging.getLogger("ialirt_data_access").setLevel(logging.INFO)


def get_version() -> str:
    try:
        return version("imap-mag")
    except PackageNotFoundError:
        print("IMAP MAG CLI Version unknown, not installed via pip.")
        return "unknown"


__version__ = get_version()
