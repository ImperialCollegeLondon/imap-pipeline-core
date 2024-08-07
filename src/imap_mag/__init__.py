"""The main module for project."""

from pathlib import Path

from single_version import get_version

__version__ = get_version("imap-mag", Path(__file__).parent.parent)
