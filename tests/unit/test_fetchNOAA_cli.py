"""Tests for fetch_noaa CLI command."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from imap_mag.cli.fetch.noaa import fetch_noaa

_PATCHES = (
    "imap_mag.cli.fetch.noaa.AppSettings",
    "imap_mag.cli.fetch.noaa.NOAAPipeline",
    "imap_mag.cli.fetch.noaa.initialiseLoggingForCommand",
)


def _make_mock_pipeline(success: bool = True) -> MagicMock:
    """Return a mock NOAAPipeline whose run() is awaitable."""
    mock_pipeline = MagicMock()
    mock_pipeline.run = AsyncMock()
    mock_pipeline.get_results.return_value = MagicMock(success=success, data_items=[])
    return mock_pipeline


class TestFetchNOAACommand:
    """Tests for the fetch_noaa CLI command."""

    def test_creates_pipeline_with_spacecraft_instrument_and_settings(self) -> None:
        # Set up.
        mock_pipeline = _make_mock_pipeline()

        with (
            patch(_PATCHES[0]) as mock_settings_cls,
            patch(_PATCHES[1]) as mock_pipeline_cls,
            patch(_PATCHES[2]),
        ):
            mock_pipeline_cls.return_value = mock_pipeline
            fetch_noaa(spacecraft="SOLAR1", instrument="mag")

        mock_pipeline_cls.assert_called_once_with(
            spacecraft="SOLAR1",
            instrument="mag",
            database=None,
            settings=mock_settings_cls.return_value,
        )

    def test_builds_and_runs_pipeline(self) -> None:
        # Set up.
        mock_pipeline = _make_mock_pipeline()

        with (
            patch(_PATCHES[0]),
            patch(_PATCHES[1], return_value=mock_pipeline),
            patch(_PATCHES[2]),
        ):
            fetch_noaa(spacecraft="SOLAR1", instrument="mag")

        mock_pipeline.build.assert_called_once()
        mock_pipeline.run.assert_called_once()

    def test_raises_runtime_error_when_pipeline_fails(self) -> None:
        # Set up.
        mock_pipeline = _make_mock_pipeline(success=False)

        with (
            patch(_PATCHES[0]),
            patch(_PATCHES[1], return_value=mock_pipeline),
            patch(_PATCHES[2]),
        ):
            with pytest.raises(RuntimeError):
                fetch_noaa(spacecraft="SOLAR1", instrument="mag")
