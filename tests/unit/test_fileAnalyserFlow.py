"""Unit tests for the file_analyser_flow run parameters and settings override."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import TypeAdapter

from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    IndexByIdsRunParameters,
    IndexByPathOrModifiedDateRunParameters,
    ProgressUpdateMode,
)
from prefect_server.fileAnalyserFlow import file_analyser_flow


def _run_parameters_adapter() -> TypeAdapter:
    return TypeAdapter(file_analyser_flow.fn.__annotations__["run_parameters"])


class TestRunParametersParsing:
    def test_default_is_automatic(self):
        result = _run_parameters_adapter().validate_python({})
        assert isinstance(result, AutomaticRunParameters)
        assert result.progress_mode == ProgressUpdateMode.AUTO_UPDATE_PROGRESS_IF_NEWER

    def test_file_ids_parsed_as_ids_params(self):
        result = _run_parameters_adapter().validate_python({"file_ids": [1, 2, 3]})
        assert isinstance(result, IndexByIdsRunParameters)
        assert result.file_ids == [1, 2, 3]
        assert result.progress_mode == ProgressUpdateMode.NEVER_UPDATE_PROGRESS

    def test_file_paths_parsed_as_path_or_date_params(self):
        result = _run_parameters_adapter().validate_python(
            {"file_paths": ["science/**/*.cdf"]}
        )
        assert isinstance(result, IndexByPathOrModifiedDateRunParameters)
        assert result.file_paths == ["science/**/*.cdf"]
        assert result.progress_mode == ProgressUpdateMode.NEVER_UPDATE_PROGRESS

    def test_modified_dates_parsed_as_path_or_date_params(self):
        result = _run_parameters_adapter().validate_python(
            {"modified_after": "2026-01-01T00:00:00Z"}
        )
        assert isinstance(result, IndexByPathOrModifiedDateRunParameters)
        assert result.modified_after == datetime(2026, 1, 1, tzinfo=UTC)

    def test_paths_and_dates_combined(self):
        result = _run_parameters_adapter().validate_python(
            {
                "file_paths": ["hk/**/*.csv"],
                "modified_after": "2026-01-01T00:00:00Z",
                "modified_before": "2026-12-31T00:00:00Z",
            }
        )
        assert isinstance(result, IndexByPathOrModifiedDateRunParameters)
        assert result.file_paths == ["hk/**/*.csv"]
        assert result.modified_before == datetime(2026, 12, 31, tzinfo=UTC)


class TestMaxFilesPerRunOverride:
    @staticmethod
    async def _run_flow(settings: MagicMock, **flow_kwargs):
        pipeline = MagicMock()
        pipeline.run = AsyncMock()
        pipeline.get_results.return_value.data_items = []

        with (
            patch("prefect_server.fileAnalyserFlow.AppSettings", return_value=settings),
            patch("prefect_server.fileAnalyserFlow.Database"),
            patch(
                "prefect_server.fileAnalyserFlow.FileAnalysisPipeline",
                return_value=pipeline,
            ),
        ):
            await file_analyser_flow.fn(**flow_kwargs)

    @pytest.mark.asyncio
    async def test_flow_argument_overrides_config_value(self):
        settings = MagicMock()
        settings.file_analysis.max_files_per_run = 500

        await self._run_flow(settings, max_files_per_run=10)

        assert settings.file_analysis.max_files_per_run == 10

    @pytest.mark.asyncio
    async def test_config_value_kept_when_flow_argument_not_provided(self):
        settings = MagicMock()
        settings.file_analysis.max_files_per_run = 500

        await self._run_flow(settings)

        assert settings.file_analysis.max_files_per_run == 500
