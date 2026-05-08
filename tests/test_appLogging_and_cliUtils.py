"""Tests for appLogging and cliUtils modules."""

import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from imap_mag.appLogging import AppLogging
from imap_mag.cli import cliUtils
from imap_mag.cli.cliUtils import fetch_file_for_work, throw_error_file_not_found


def _remove_framework_handlers(root_logger):
    """Remove pytest and Prefect handlers so appLogging.set_up_logging can proceed."""
    root_logger.handlers = [
        h for h in root_logger.handlers
        if not any(
            prefix in (type(h).__module__ + "." + type(h).__name__)
            for prefix in ("_pytest.logging", "prefect.logging")
        )
    ]


class TestAppLogging:
    """Tests for AppLogging setup and LogFormatter."""

    def test_log_formatter_with_color_adds_color_codes(self):
        formatter = AppLogging.LogFormatter(color=True, fmt="%(color_on)s%(message)s%(color_off)s")
        record = logging.LogRecord(
            name="test", level=logging.ERROR, pathname="", lineno=0,
            msg="test message", args=(), exc_info=None
        )
        formatted = formatter.format(record)
        assert "\033[" in formatted

    def test_log_formatter_without_color_has_empty_color_codes(self):
        formatter = AppLogging.LogFormatter(color=False, fmt="%(color_on)s%(message)s%(color_off)s")
        record = logging.LogRecord(
            name="test", level=logging.ERROR, pathname="", lineno=0,
            msg="test message", args=(), exc_info=None
        )
        formatted = formatter.format(record)
        assert "\033[" not in formatted

    def test_log_formatter_color_applied_for_all_standard_levels(self):
        formatter = AppLogging.LogFormatter(color=True, fmt="%(color_on)s%(message)s%(color_off)s")
        for level in (logging.CRITICAL, logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG):
            record = logging.LogRecord(
                name="test", level=level, pathname="", lineno=0,
                msg="msg", args=(), exc_info=None
            )
            formatted = formatter.format(record)
            assert "\033[" in formatted

    def test_reset_setup_flag_allows_reconfiguration(self, tmp_path):
        """Tests that reset_setup_flag allows setup to run again."""
        AppLogging.reset_setup_flag()

        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers[:]
        try:
            # Temporarily remove pytest handlers so set_up_logging can proceed
            _remove_framework_handlers(root_logger)

            log_file = tmp_path / "test.log"
            result = AppLogging.set_up_logging(
                console_log_output="stdout",
                console_log_level="info",
                console_log_color=False,
                logfile_file=str(log_file),
                logfile_log_level="debug",
                logfile_log_color=False,
                log_line_template="%(message)s",
                console_log_line_template="%(message)s",
            )
            assert result is True
        finally:
            root_logger.handlers = original_handlers
            AppLogging.reset_setup_flag()

    def test_setup_logging_invalid_console_output_returns_false(self, tmp_path):
        """Tests that invalid console output returns False."""
        AppLogging.reset_setup_flag()

        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers[:]
        try:
            _remove_framework_handlers(root_logger)

            log_file = tmp_path / "test.log"
            result = AppLogging.set_up_logging(
                console_log_output="invalid_output",
                console_log_level="info",
                console_log_color=False,
                logfile_file=str(log_file),
                logfile_log_level="debug",
                logfile_log_color=False,
                log_line_template="%(message)s",
                console_log_line_template="%(message)s",
            )
            assert result is False
        finally:
            root_logger.handlers = original_handlers
            AppLogging.reset_setup_flag()

    def test_setup_logging_invalid_console_level_returns_false(self, tmp_path):
        """Tests that invalid console log level returns False."""
        AppLogging.reset_setup_flag()

        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers[:]
        try:
            _remove_framework_handlers(root_logger)

            log_file = tmp_path / "test.log"
            result = AppLogging.set_up_logging(
                console_log_output="stdout",
                console_log_level="not_a_level",
                console_log_color=False,
                logfile_file=str(log_file),
                logfile_log_level="debug",
                logfile_log_color=False,
                log_line_template="%(message)s",
                console_log_line_template="%(message)s",
            )
            assert result is False
        finally:
            root_logger.handlers = original_handlers
            AppLogging.reset_setup_flag()

    def test_setup_logging_invalid_log_file_path_returns_false(self, tmp_path):
        """Tests that unreachable log file returns False."""
        AppLogging.reset_setup_flag()

        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers[:]
        try:
            _remove_framework_handlers(root_logger)

            result = AppLogging.set_up_logging(
                console_log_output="stdout",
                console_log_level="info",
                console_log_color=False,
                logfile_file="/nonexistent/directory/test.log",
                logfile_log_level="debug",
                logfile_log_color=False,
                log_line_template="%(message)s",
                console_log_line_template="%(message)s",
            )
            assert result is False
        finally:
            root_logger.handlers = original_handlers
            AppLogging.reset_setup_flag()

    def test_setup_logging_skips_when_pytest_handler_present(self, tmp_path):
        """Tests that setup is skipped when running under pytest (due to pytest handler detection)."""
        AppLogging.reset_setup_flag()

        root_logger = logging.getLogger()
        pytest_handler = logging.StreamHandler()
        pytest_handler.__class__ = type("LogCaptureHandler", (logging.StreamHandler,), {"__module__": "_pytest.logging"})
        root_logger.addHandler(pytest_handler)

        try:
            log_file = tmp_path / "test.log"
            result = AppLogging.set_up_logging(
                console_log_output="stdout",
                console_log_level="info",
                console_log_color=False,
                logfile_file=str(log_file),
                logfile_log_level="debug",
                logfile_log_color=False,
                log_line_template="%(message)s",
                console_log_line_template="%(message)s",
            )
            assert result is True  # skips silently
        finally:
            root_logger.handlers.remove(pytest_handler)
            AppLogging.reset_setup_flag()

    def test_setup_logging_second_call_skips(self, tmp_path, capsys):
        """Tests that second call to set_up_logging is a no-op."""
        AppLogging.reset_setup_flag()

        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers[:]
        try:
            _remove_framework_handlers(root_logger)

            log_file = tmp_path / "test.log"
            kwargs = dict(
                console_log_output="stdout",
                console_log_level="info",
                console_log_color=False,
                logfile_file=str(log_file),
                logfile_log_level="debug",
                logfile_log_color=False,
                log_line_template="%(message)s",
                console_log_line_template="%(message)s",
            )
            AppLogging.set_up_logging(**kwargs)
            # Clear handlers back (the first call added handlers)
            _remove_framework_handlers(root_logger)

            result = AppLogging.set_up_logging(**kwargs)
            assert result is True
            output = capsys.readouterr().out
            assert "already set up" in output
        finally:
            root_logger.handlers = original_handlers
            AppLogging.reset_setup_flag()


    def test_setup_logging_with_stderr_output_succeeds(self, tmp_path):
        """Tests that stderr output option works."""
        AppLogging.reset_setup_flag()

        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers[:]
        try:
            _remove_framework_handlers(root_logger)

            log_file = tmp_path / "test.log"
            result = AppLogging.set_up_logging(
                console_log_output="stderr",
                console_log_level="info",
                console_log_color=False,
                logfile_file=str(log_file),
                logfile_log_level="debug",
                logfile_log_color=False,
                log_line_template="%(message)s",
                console_log_line_template="%(message)s",
            )
            assert result is True
        finally:
            root_logger.handlers = original_handlers
            AppLogging.reset_setup_flag()

    def test_setup_logging_invalid_logfile_level_returns_false(self, tmp_path):
        """Tests that invalid log file level returns False."""
        AppLogging.reset_setup_flag()

        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers[:]
        try:
            _remove_framework_handlers(root_logger)

            log_file = tmp_path / "test.log"
            result = AppLogging.set_up_logging(
                console_log_output="stdout",
                console_log_level="info",
                console_log_color=False,
                logfile_file=str(log_file),
                logfile_log_level="not_a_level",
                logfile_log_color=False,
                log_line_template="%(message)s",
                console_log_line_template="%(message)s",
            )
            assert result is False
        finally:
            root_logger.handlers = original_handlers
            AppLogging.reset_setup_flag()


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

        import time

        old_file = source_folder / "data_v1.csv"
        old_file.write_text("old")
        time.sleep(0.01)
        new_file = source_folder / "data_v2.csv"
        new_file.write_text("new")

        result = fetch_file_for_work(
            source_folder / "data_*.csv", work_folder, throw_if_not_found=True
        )

        assert result is not None
        assert result.name == "data_v2.csv"

    def test_returns_existing_work_file_without_copying_when_already_there(self, tmp_path):
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
        """Tests that verbose=True sets debug-level loggers."""
        original_verbose = cliUtils.globalState["verbose"]
        cliUtils.globalState["verbose"] = True
        try:
            with patch("imap_mag.appLogging.AppLogging.set_up_logging", return_value=True):
                cliUtils.initialiseLoggingForCommand(tmp_path)

            assert logging.getLogger("mag_toolkit").level == logging.DEBUG
            assert logging.getLogger("imap_mag").level == logging.DEBUG
        finally:
            cliUtils.globalState["verbose"] = original_verbose

    def test_aborts_when_logging_setup_fails(self, tmp_path):
        """Tests that typer.Abort is raised when set_up_logging fails."""
        import typer

        with (
            patch("imap_mag.appLogging.AppLogging.set_up_logging", return_value=False),
            pytest.raises(typer.Abort),
        ):
            cliUtils.initialiseLoggingForCommand(tmp_path)


class TestFetchFileForWorkWithPatternDate:
    def test_percent_pattern_in_filename_is_resolved_with_current_date(self, tmp_path):
        """Tests that %Y%m%d patterns in filenames are expanded to today's date."""
        from datetime import datetime

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


class TestSequenceablePathHandlerAddMetadata:
    def test_add_metadata_raises_not_implemented_error(self):
        """Tests that add_metadata raises NotImplementedError on base class."""
        from datetime import datetime

        from imap_mag.io.file.HKBinaryPathHandler import HKBinaryPathHandler

        handler = HKBinaryPathHandler(
            descriptor="hsk-pw",
            content_date=datetime(2025, 1, 1),
            extension="pkts",
        )

        with pytest.raises(NotImplementedError):
            handler.add_metadata({"key": "value"})

    def test_get_metadata_returns_none_by_default(self):
        """Tests that get_metadata returns None on base class."""
        from datetime import datetime

        from imap_mag.io.file.HKBinaryPathHandler import HKBinaryPathHandler

        handler = HKBinaryPathHandler(
            descriptor="hsk-pw",
            content_date=datetime(2025, 1, 1),
            extension="pkts",
        )

        assert handler.get_metadata() is None
