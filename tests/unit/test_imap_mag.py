"""Tests for imap_mag package entry points."""

from importlib.metadata import PackageNotFoundError
from unittest.mock import patch

from imap_mag import get_version


class TestImapMagVersion:
    def test_get_version_returns_unknown_when_not_installed(self):
        with patch("imap_mag.version", side_effect=PackageNotFoundError):
            result = get_version()

        assert result == "unknown"
