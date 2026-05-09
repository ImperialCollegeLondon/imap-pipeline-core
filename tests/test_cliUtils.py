"""Tests for cliUtils module."""

import logging
import os
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
import typer

from imap_mag.cli import cliUtils
from imap_mag.cli.cliUtils import fetch_file_for_work, throw_error_file_not_found


class TestThrowErrorFileNotFound:
    def test_raises_file_not_found_error(self):
        with pytest.raises(FileNotFoundError, match="Unable to find file"):
            throw_error_file_not_found(Path("/some/folder"), "*.csv")

    def test_error_message_contains_folder_and_filename(self):
        with pytest.raises(FileNotFoundError) as exc_info:
            throw_error_file_not_found(Path("/data/store"), "my_file.csv")
        assert "/data/store" in str(exc_info.value)
        assert "my_file.csv" in str(exc_info.value)


class TestFetchFileForWork:
    def test_returns_none_when_source_folder_does_not_exist(self, tmp_path):
        non_existent = tmp_path / "nonexistent_folder"
        result = fetch_file_for_work(
            non_existent / "file.csv", tmp_path, throw_if_not_found=False
        )
        assert result is None

    def test_raises_when_folder_does_not_exist_and_throw_is_true(self, tmp_path):
        non_existent = tmp_path / "nonexistent_folder"
        with pytest.raises(FileNotFoundError):
            fetch_file_for_work(
                non_existent / "file.csv", tmp_path, throw_if_not_found=True
            )

    def test_copies_matching_file_to_work_folder(self, tmp_path):
        source_folder = tmp_path / "source"
        source_folder.mkdir()
        work_folder = tmp_path / "work"
        work_folder.mkdir()
        source_file = source_folder / "data.csv"
        source_file.write_text("hello")

        result = fetch_file_for_work(
            source_folder / "data.csv", work_folder, throw_if_not_found=True
        )

        assert result is not None
        assert result.name == "data.csv"
        assert (work_folder / "data.csv").exists()

    def test_raises_when_no_matching_file(self, tmp_path):
        source_folder = tmp_path / "source"
        source_folder.mkdir()
        work_folder = tmp_path / "work"
        work_folder.mkdir()

        with pytest.raises(FileNotFoundError):
            fetch_file_for_work(
                source_folder / "nonexistent.csv", work_folder, throw_if_not_found=True
            )

    def test_returns_most_recently_modified_file_when_multiple_match(self, tmp_path):
        source_folder = tmp_path / "source"
        source_folder.mkdir()
        work_folder = tmp_path / "work"
        work_folder.mkdir()

        old_file = source_folder / "data_v1.csv"
        old_file.write_text("old")
        os.utime(old_file, (1000, 1000))
        new_file = source_folder / "data_v2.csv"
        new_file.write_text("new")
        os.utime(new_file, (2000, 2000))

        result = fetch_file_for_work(
            source_folder / "data_*.csv", work_folder, throw_if_not_found=True
        )

        assert result is not None
        assert result.name == "data_v2.csv"

    def test_returns_existing_work_file_without_copying_when_already_there(
        self, tmp_path
    ):
        work_folder = tmp_path / "work"
        work_folder.mkdir()
        work_file = work_folder / "data.csv"
        work_file.write_text("already here")

        result = fetch_file_for_work(
            work_folder / "data.csv", work_folder, throw_if_not_found=True
        )

        assert result is not None
        assert result.resolve() == work_file.resolve()


class TestInitialiseLoggingForCommand:
    def test_verbose_mode_sets_debug_loggers_and_format(self, tmp_path):
        original_verbose = cliUtils.globalState["verbose"]
        cliUtils.globalState["verbose"] = True
        try:
            with patch(
                "imap_mag.appLogging.AppLogging.set_up_logging", return_value=True
            ):
                cliUtils.initialiseLoggingForCommand(tmp_path)

            assert logging.getLogger("mag_toolkit").level == logging.DEBUG
            assert logging.getLogger("imap_mag").level == logging.DEBUG
        finally:
            cliUtils.globalState["verbose"] = original_verbose

    def test_aborts_when_logging_setup_fails(self, tmp_path):
        with (
            patch("imap_mag.appLogging.AppLogging.set_up_logging", return_value=False),
            pytest.raises(typer.Abort),
        ):
            cliUtils.initialiseLoggingForCommand(tmp_path)


class TestFetchFileForWorkWithPatternDate:
    def test_percent_pattern_in_filename_is_resolved_with_current_date(self, tmp_path):
        source_folder = tmp_path / "source"
        source_folder.mkdir()
        work_folder = tmp_path / "work"
        work_folder.mkdir()

        today_str = datetime.now().strftime("%Y%m%d")
        source_file = source_folder / f"data_{today_str}.csv"
        source_file.write_text("content")

        result = fetch_file_for_work(
            source_folder / "data_%Y%m%d.csv", work_folder, throw_if_not_found=True
        )

        assert result is not None
        assert result.name == f"data_{today_str}.csv"
