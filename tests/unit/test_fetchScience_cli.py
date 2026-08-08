"""Tests for fetch science CLI command."""

import hashlib
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from imap_db.model import File
from imap_mag.cli.fetch.science import fetch_science
from imap_mag.config import AppSettings, DatastoreSaveOption, FetchMode
from imap_mag.io import DatastoreFileManager, DBIndexedDatastoreFileManager
from imap_mag.io.file import SciencePathHandler


class TestFetchScience:
    def test_fetch_science_returns_empty_when_no_data(
        self, dynamic_work_folder, clean_datastore
    ):
        mock_sdc = MagicMock()
        mock_fetch_science = MagicMock()
        mock_fetch_science.download_science.return_value = {}

        with (
            patch("imap_mag.cli.fetch.science.SDCDataAccess", return_value=mock_sdc),
            patch(
                "imap_mag.cli.fetch.science.FetchScience",
                return_value=mock_fetch_science,
            ),
            patch("imap_mag.cli.fetch.science.initialiseLoggingForCommand"),
        ):
            result = fetch_science(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
            )

        assert result == {}


class TestFetchScienceOverwriteOption:
    """Tests for DatastoreSaveOption in fetch_science covering no-DB and DB cases."""

    _LEVEL = "l1c"
    _DESCRIPTOR = "norm-magi"
    _DATE = datetime(2025, 5, 2)
    _VERSION = 0
    _VERSION_MAJOR = 1

    def _create_existing_file_in_datastore(
        self, datastore: Path, content: str = "original content"
    ) -> SciencePathHandler:
        """Place a science file in the datastore directory and return its handler."""
        handler = SciencePathHandler(
            level=self._LEVEL,
            descriptor=self._DESCRIPTOR,
            content_date=self._DATE,
            version=self._VERSION,
            version_major=self._VERSION_MAJOR,
            has_major_version=True,
            extension="cdf",
        )
        folder = datastore / handler.get_folder_structure()
        folder.mkdir(parents=True, exist_ok=True)
        (folder / handler.get_filename()).write_text(content)
        return handler

    def _make_downloaded_handler(self) -> SciencePathHandler:
        """Return a locked handler as FetchScience would produce."""
        return SciencePathHandler(
            level=self._LEVEL,
            descriptor=self._DESCRIPTOR,
            content_date=self._DATE,
            version=self._VERSION,
            version_major=self._VERSION_MAJOR,
            has_major_version=True,
            extension="cdf",
            version_is_locked=True,
        )

    def _make_downloaded_file(
        self, work_folder: Path, content: str = "updated content"
    ) -> Path:
        """Create a temporary file that represents a freshly downloaded science file."""
        downloaded = work_folder / "downloaded_science.cdf"
        downloaded.write_text(content)
        return downloaded

    def _db_backed_manager(
        self, settings: AppSettings
    ) -> DBIndexedDatastoreFileManager:
        """Return a DBIndexedDatastoreFileManager using a mock database."""
        mock_db = MagicMock()
        mock_db.get_files.return_value = []
        real_dsm = DatastoreFileManager(settings)
        return DBIndexedDatastoreFileManager(
            real_dsm, database=mock_db, settings=settings
        )

    # ── No-database (DownloadOnly) tests ────────────────────────────────────

    def test_blocked_raises_when_same_version_different_content_no_db(
        self, dynamic_work_folder, clean_datastore
    ):
        """FILE_OVERWRITES_BLOCKED raises ValueError when the same-version file has different content (no DB)."""
        self._create_existing_file_in_datastore(clean_datastore)
        downloaded_file = self._make_downloaded_file(dynamic_work_folder)
        handler = self._make_downloaded_handler()

        mock_fetch = MagicMock()
        mock_fetch.download_science.return_value = {downloaded_file: handler}

        with (
            patch("imap_mag.cli.fetch.science.SDCDataAccess"),
            patch("imap_mag.cli.fetch.science.FetchScience", return_value=mock_fetch),
            patch("imap_mag.cli.fetch.science.initialiseLoggingForCommand"),
            pytest.raises(ValueError, match="cannot be changed"),
        ):
            fetch_science(
                start_date=self._DATE,
                end_date=self._DATE,
                fetch_mode=FetchMode.DownloadOnly,
                overwrite_option=DatastoreSaveOption.FILE_OVERWRITES_BLOCKED,
            )

    def test_allowed_overwrites_same_version_different_content_no_db(
        self, dynamic_work_folder, clean_datastore
    ):
        """FILE_OVERWRITES_ALLOWED silently replaces the file at the same version (no DB)."""
        self._create_existing_file_in_datastore(
            clean_datastore, content="original content"
        )
        downloaded_file = self._make_downloaded_file(
            dynamic_work_folder, content="updated content"
        )
        handler = self._make_downloaded_handler()

        mock_fetch = MagicMock()
        mock_fetch.download_science.return_value = {downloaded_file: handler}

        with (
            patch("imap_mag.cli.fetch.science.SDCDataAccess"),
            patch("imap_mag.cli.fetch.science.FetchScience", return_value=mock_fetch),
            patch("imap_mag.cli.fetch.science.initialiseLoggingForCommand"),
        ):
            result = fetch_science(
                start_date=self._DATE,
                end_date=self._DATE,
                fetch_mode=FetchMode.DownloadOnly,
                overwrite_option=DatastoreSaveOption.FILE_OVERWRITES_ALLOWED,
            )

        assert len(result) == 1
        output_file = next(iter(result.keys()))
        assert output_file.read_text() == "updated content"
        assert result[output_file].version == self._VERSION
        # version_is_locked must remain True even in ALLOWED mode — the version
        # number is authoritative from SDC and must never change.
        assert result[output_file].version_is_locked is True

    # ── Database-used (DownloadAndUpdateProgress) tests ─────────────────────

    def test_blocked_raises_when_same_version_different_content_with_db(
        self, dynamic_work_folder, clean_datastore
    ):
        """FILE_OVERWRITES_BLOCKED raises ValueError when the same-version file has different content (with DB)."""
        self._create_existing_file_in_datastore(clean_datastore)
        downloaded_file = self._make_downloaded_file(dynamic_work_folder)
        handler = self._make_downloaded_handler()

        mock_fetch = MagicMock()
        mock_fetch.download_science.return_value = {downloaded_file: handler}

        with (
            patch("imap_mag.cli.fetch.science.SDCDataAccess"),
            patch("imap_mag.cli.fetch.science.FetchScience", return_value=mock_fetch),
            patch("imap_mag.cli.fetch.science.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.fetch.science.DatastoreFileManager.CreateByMode",
                return_value=self._db_backed_manager(AppSettings()),  # type: ignore
            ),
            pytest.raises(ValueError, match="cannot be changed"),
        ):
            fetch_science(
                start_date=self._DATE,
                end_date=self._DATE,
                fetch_mode=FetchMode.DownloadAndUpdateProgress,
                overwrite_option=DatastoreSaveOption.FILE_OVERWRITES_BLOCKED,
            )

    def test_allowed_overwrites_same_version_different_content_with_db(
        self, dynamic_work_folder, clean_datastore
    ):
        """FILE_OVERWRITES_ALLOWED replaces the file and upserts the DB record (with DB)."""
        self._create_existing_file_in_datastore(
            clean_datastore, content="original content"
        )
        downloaded_file = self._make_downloaded_file(
            dynamic_work_folder, content="updated content"
        )
        handler = self._make_downloaded_handler()

        mock_fetch = MagicMock()
        mock_fetch.download_science.return_value = {downloaded_file: handler}

        db_manager = self._db_backed_manager(AppSettings())  # type: ignore

        with (
            patch("imap_mag.cli.fetch.science.SDCDataAccess"),
            patch("imap_mag.cli.fetch.science.FetchScience", return_value=mock_fetch),
            patch("imap_mag.cli.fetch.science.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.fetch.science.DatastoreFileManager.CreateByMode",
                return_value=db_manager,
            ),
        ):
            result = fetch_science(
                start_date=self._DATE,
                end_date=self._DATE,
                fetch_mode=FetchMode.DownloadAndUpdateProgress,
                overwrite_option=DatastoreSaveOption.FILE_OVERWRITES_ALLOWED,
            )

        assert len(result) == 1
        output_file = next(iter(result.keys()))
        assert output_file.read_text() == "updated content"
        assert result[output_file].version == self._VERSION
        # version_is_locked must remain True even in ALLOWED mode.
        assert result[output_file].version_is_locked is True
        db_manager._DBIndexedDatastoreFileManager__database.upsert_file.assert_called_once()

    def test_allowed_with_db_rejects_version_reassignment_even_for_same_hash(
        self, dynamic_work_folder, clean_datastore
    ):
        """FILE_OVERWRITES_ALLOWED + version_is_locked blocks silent version reassignment.

        If the DB already holds the same content (same hash) at a *different* version,
        the handler's version_is_locked=True must prevent set_sequence() from silently
        adopting that version — the SDC-assigned version is authoritative.
        """
        self._create_existing_file_in_datastore(
            clean_datastore, content="original content"
        )
        new_content = b"updated content"
        downloaded_file = self._make_downloaded_file(
            dynamic_work_folder, content=new_content.decode()
        )
        handler = self._make_downloaded_handler()

        # DB has the same hash as the new file but at a DIFFERENT version (99 vs 0).
        # The path must contain the expected folder structure so the DB filter passes.
        content_hash = hashlib.md5(new_content).hexdigest()
        folder_structure = handler.get_folder_structure()
        db_record_at_wrong_version = File(
            name="imap_mag_l1c_norm-magi_20250502_v001.0099.cdf",
            path=f"{folder_structure}/imap_mag_l1c_norm-magi_20250502_v001.0099.cdf",
            descriptor="norm-magi",
            version=99,
            version_major=self._VERSION_MAJOR,
            hash=content_hash,
            size=len(new_content),
            content_date=self._DATE,
            deletion_date=None,
            software_version="0.0.0",
        )

        mock_fetch = MagicMock()
        mock_fetch.download_science.return_value = {downloaded_file: handler}

        mock_db = MagicMock()
        mock_db.get_files.return_value = [db_record_at_wrong_version]
        real_dsm = DatastoreFileManager(AppSettings())  # type: ignore
        db_manager = DBIndexedDatastoreFileManager(
            real_dsm,
            database=mock_db,
            settings=AppSettings(),  # type: ignore
        )

        with (
            patch("imap_mag.cli.fetch.science.SDCDataAccess"),
            patch("imap_mag.cli.fetch.science.FetchScience", return_value=mock_fetch),
            patch("imap_mag.cli.fetch.science.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.fetch.science.DatastoreFileManager.CreateByMode",
                return_value=db_manager,
            ),
            pytest.raises(ValueError, match="cannot be changed"),
        ):
            fetch_science(
                start_date=self._DATE,
                end_date=self._DATE,
                fetch_mode=FetchMode.DownloadAndUpdateProgress,
                overwrite_option=DatastoreSaveOption.FILE_OVERWRITES_ALLOWED,
            )
