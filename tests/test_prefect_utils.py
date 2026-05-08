"""Tests for prefect_server utility functions."""

import logging
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from prefect_server.prefectUtils import get_cron_from_env, try_get_prefect_logger


class TestGetCronFromEnv:
    def test_returns_none_when_env_var_not_set(self):
        with patch.dict(os.environ, {}, clear=True):
            result = get_cron_from_env("IMAP_CRON_SOME_FLOW")
        assert result is None

    def test_returns_none_when_env_var_is_empty(self):
        with patch.dict(os.environ, {"IMAP_CRON_SOME_FLOW": ""}):
            result = get_cron_from_env("IMAP_CRON_SOME_FLOW")
        assert result is None

    def test_returns_cron_string_when_set(self):
        with patch.dict(os.environ, {"IMAP_CRON_SOME_FLOW": "0 * * * *"}):
            result = get_cron_from_env("IMAP_CRON_SOME_FLOW")
        assert result == "0 * * * *"

    def test_strips_surrounding_quotes_and_spaces(self):
        with patch.dict(os.environ, {"IMAP_CRON_SOME_FLOW": "'0 * * * *'"}):
            result = get_cron_from_env("IMAP_CRON_SOME_FLOW")
        assert result == "0 * * * *"

    def test_returns_default_when_env_var_not_set(self):
        with patch.dict(os.environ, {}, clear=True):
            result = get_cron_from_env("IMAP_CRON_SOME_FLOW", default="0 0 * * *")
        assert result == "0 0 * * *"

    def test_returns_none_when_default_is_none(self):
        with patch.dict(os.environ, {}, clear=True):
            result = get_cron_from_env("IMAP_CRON_SOME_FLOW", default=None)
        assert result is None


class TestTryGetPrefectLogger:
    def test_returns_module_logger_outside_prefect_context(self):
        logger = try_get_prefect_logger("test.module")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test.module"

    def test_returns_prefect_logger_in_prefect_context(self):
        mock_prefect_logger = MagicMock()
        with patch("prefect_server.prefectUtils.get_run_logger", return_value=mock_prefect_logger):
            logger = try_get_prefect_logger("test.module")
        assert logger is mock_prefect_logger
