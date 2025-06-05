"""The main module for project."""

from importlib.metadata import PackageNotFoundError, version


def get_version() -> str:
    try:
        return version("imap-mag")
    except PackageNotFoundError:
        print("IMAP MAG CLI Version unknown, not installed via pip.")
        return "unknown"


__version__ = get_version()
