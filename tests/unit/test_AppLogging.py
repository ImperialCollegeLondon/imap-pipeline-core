"""Tests for AppLogging setup and LogFormatter."""

import logging

from imap_mag.appLogging import AppLogging


def _remove_framework_handlers(root_logger):
    root_logger.handlers = [
        h
        for h in root_logger.handlers
        if not any(
            prefix in (type(h).__module__ + "." + type(h).__name__)
            for prefix in ("_pytest.logging", "prefect.logging")
        )
    ]


class TestAppLogging:
    def test_log_formatter_with_color_adds_color_codes(self):
        formatter = AppLogging.LogFormatter(
            color=True, fmt="%(color_on)s%(message)s%(color_off)s"
        )
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="test message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "\033[" in formatted

    def test_log_formatter_without_color_has_empty_color_codes(self):
        formatter = AppLogging.LogFormatter(
            color=False, fmt="%(color_on)s%(message)s%(color_off)s"
        )
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="test message",
            args=(),
            exc_info=None,
        )
        formatted = formatter.format(record)
        assert "\033[" not in formatted

    def test_log_formatter_color_applied_for_all_standard_levels(self):
        formatter = AppLogging.LogFormatter(
            color=True, fmt="%(color_on)s%(message)s%(color_off)s"
        )
        for level in (
            logging.CRITICAL,
            logging.ERROR,
            logging.WARNING,
            logging.INFO,
            logging.DEBUG,
        ):
            record = logging.LogRecord(
                name="test",
                level=level,
                pathname="",
                lineno=0,
                msg="msg",
                args=(),
                exc_info=None,
            )
            formatted = formatter.format(record)
            assert "\033[" in formatted

    def test_reset_setup_flag_allows_reconfiguration(self, tmp_path):
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
        AppLogging.reset_setup_flag()

        root_logger = logging.getLogger()
        pytest_handler = logging.StreamHandler()
        pytest_handler.__class__ = type(
            "LogCaptureHandler",
            (logging.StreamHandler,),
            {"__module__": "_pytest.logging"},
        )
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
            assert result is True
        finally:
            root_logger.handlers.remove(pytest_handler)
            AppLogging.reset_setup_flag()

    def test_setup_logging_second_call_skips(self, tmp_path, capsys):
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
            _remove_framework_handlers(root_logger)

            result = AppLogging.set_up_logging(**kwargs)
            assert result is True
            output = capsys.readouterr().out
            assert "already set up" in output
        finally:
            root_logger.handlers = original_handlers
            AppLogging.reset_setup_flag()

    def test_setup_logging_with_stderr_output_succeeds(self, tmp_path):
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
