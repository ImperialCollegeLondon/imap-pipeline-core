"""Tests for prefect_server utility functions."""

import asyncio
import logging
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from prefect_server.prefectUtils import (
    get_cron_from_env,
    get_secret_block,
    get_secret_or_env_var,
    try_get_prefect_logger,
)


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
        with patch(
            "prefect_server.prefectUtils.get_run_logger",
            return_value=mock_prefect_logger,
        ):
            logger = try_get_prefect_logger("test.module")
        assert logger is mock_prefect_logger


class TestGetSecretOrEnvVar:
    def test_returns_env_var_when_not_in_prefect_context(self):
        with patch.dict(os.environ, {"MY_VAR": "my_value"}):
            with (
                patch("prefect.context.FlowRunContext.get", return_value=None),
                patch("prefect.context.TaskRunContext.get", return_value=None),
            ):
                result = asyncio.get_event_loop().run_until_complete(
                    get_secret_or_env_var("secret_name", "MY_VAR")
                )

        assert result == "my_value"

    def test_raises_when_neither_secret_nor_env_var_available(self):
        with (
            patch("prefect.context.FlowRunContext.get", return_value=None),
            patch("prefect.context.TaskRunContext.get", return_value=None),
        ):
            env_backup = os.environ.pop("MISSING_VAR", None)
            try:
                with pytest.raises(ValueError, match="both undefined"):
                    asyncio.get_event_loop().run_until_complete(
                        get_secret_or_env_var("secret_name", "MISSING_VAR")
                    )
            finally:
                if env_backup is not None:
                    os.environ["MISSING_VAR"] = env_backup


class TestGetSecretBlock:
    def test_raises_when_secret_block_is_empty(self):
        mock_secret = MagicMock()
        mock_secret.get.return_value = None

        with patch(
            "prefect_server.prefectUtils.Secret.aload",
            new_callable=AsyncMock,
            return_value=mock_secret,
        ):
            with pytest.raises(ValueError, match="empty"):
                asyncio.get_event_loop().run_until_complete(
                    get_secret_block("empty_secret")
                )

    def test_returns_value_when_secret_exists(self):
        mock_secret = MagicMock()
        mock_secret.get.return_value = "my_secret_value"

        with patch(
            "prefect_server.prefectUtils.Secret.aload",
            new_callable=AsyncMock,
            return_value=mock_secret,
        ):
            result = asyncio.get_event_loop().run_until_complete(
                get_secret_block("my_secret")
            )

        assert result == "my_secret_value"

    def test_raises_when_secret_block_not_found(self):
        with patch(
            "prefect_server.prefectUtils.Secret.aload",
            new_callable=AsyncMock,
            side_effect=ValueError("Block not found"),
        ):
            with pytest.raises(ValueError):
                asyncio.get_event_loop().run_until_complete(
                    get_secret_block("nonexistent_secret")
                )
