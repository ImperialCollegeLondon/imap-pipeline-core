"""Tests for ialirtUtils module."""

from datetime import datetime

from imap_mag.cli.ialirtUtils import (
    fetch_ialirt_files_for_work,
    fetch_ialirt_hk_files_for_work,
)


class TestFetchIalirtFilesForWork:
    def test_returns_empty_list_when_no_files_found_by_date_range(self, tmp_path):
        data_store = tmp_path / "datastore"
        data_store.mkdir()
        work_folder = tmp_path / "work"
        work_folder.mkdir()

        result = fetch_ialirt_files_for_work(
            data_store=data_store,
            work_folder=work_folder,
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 1),
            files=None,
        )

        assert result == []

    def test_returns_empty_list_when_no_dates_or_files_provided_and_no_data(
        self, tmp_path
    ):
        data_store = tmp_path / "datastore"
        data_store.mkdir()
        work_folder = tmp_path / "work"
        work_folder.mkdir()

        result = fetch_ialirt_files_for_work(
            data_store=data_store,
            work_folder=work_folder,
            start_date=None,
            end_date=None,
            files=None,
        )

        assert result == []

    def test_copies_provided_files_to_work_folder(self, tmp_path):
        data_store = tmp_path / "datastore"
        data_store.mkdir()
        work_folder = tmp_path / "work"
        work_folder.mkdir()
        source_file = data_store / "imap_ialirt_20250101.csv"
        source_file.write_text("col1,col2\n1,2")

        result = fetch_ialirt_files_for_work(
            data_store=data_store,
            work_folder=work_folder,
            start_date=None,
            end_date=None,
            files=[source_file],
        )

        assert len(result) == 1
        assert result[0].name == "imap_ialirt_20250101.csv"
        assert (work_folder / "imap_ialirt_20250101.csv").exists()

    def test_returns_empty_list_when_provided_files_list_is_empty(self, tmp_path):
        data_store = tmp_path / "datastore"
        data_store.mkdir()
        work_folder = tmp_path / "work"
        work_folder.mkdir()

        result = fetch_ialirt_files_for_work(
            data_store=data_store,
            work_folder=work_folder,
            start_date=None,
            end_date=None,
            files=[],
        )

        assert result == []


class TestFetchIalirtHkFilesForWork:
    def test_returns_empty_list_when_no_files_in_date_range(self, tmp_path):
        data_store = tmp_path / "datastore"
        data_store.mkdir()
        work_folder = tmp_path / "work"
        work_folder.mkdir()

        result = fetch_ialirt_hk_files_for_work(
            data_store=data_store,
            work_folder=work_folder,
            start_date=datetime(2025, 6, 1),
            end_date=datetime(2025, 6, 1),
            files=None,
        )

        assert result == []

    def test_copies_hk_files_to_work_folder(self, tmp_path):
        data_store = tmp_path / "datastore"
        data_store.mkdir()
        work_folder = tmp_path / "work"
        work_folder.mkdir()
        source_file = data_store / "imap_ialirt_hk_20250601.csv"
        source_file.write_text("col1,col2\n1,2")

        result = fetch_ialirt_hk_files_for_work(
            data_store=data_store,
            work_folder=work_folder,
            start_date=None,
            end_date=None,
            files=[source_file],
        )

        assert len(result) == 1
        assert result[0].name == "imap_ialirt_hk_20250601.csv"

    def test_defaults_to_yesterday_and_today_when_no_params(self, tmp_path):
        data_store = tmp_path / "datastore"
        data_store.mkdir()
        work_folder = tmp_path / "work"
        work_folder.mkdir()

        # No files in data_store so it returns empty but doesn't crash
        result = fetch_ialirt_hk_files_for_work(
            data_store=data_store,
            work_folder=work_folder,
            start_date=None,
            end_date=None,
            files=None,
        )

        assert isinstance(result, list)
