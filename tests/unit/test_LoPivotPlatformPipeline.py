"""Tests for WebTCADTelemetryItemPipeline with LO_PIVOT_PLATFORM_ANGLE item."""

from datetime import datetime
from unittest.mock import MagicMock

from imap_mag.client.WebTCADLaTiS import HKWebTCADItems
from imap_mag.data_pipelines import FetchByDatesRunParameters
from imap_mag.data_pipelines.WebTCADTelemetryItemPipeline import (
    WebTCADTelemetryItemPipeline,
)


class TestLoPivotPlatformPipeline:
    def test_progress_item_id_is_lo_pivot_platform(self):
        mock_settings = MagicMock()
        pipeline = WebTCADTelemetryItemPipeline(
            item=HKWebTCADItems.LO_PIVOT_PLATFORM_ANGLE,
            database=None,
            settings=mock_settings,
        )
        assert pipeline.progress_item_id == "LO_PIVOT_PLATFORM_ANGLE"

    def test_build_creates_stages(self):
        mock_settings = MagicMock()
        pipeline = WebTCADTelemetryItemPipeline(
            item=HKWebTCADItems.LO_PIVOT_PLATFORM_ANGLE,
            database=None,
            settings=mock_settings,
        )
        run_params = FetchByDatesRunParameters(
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 31),
        )
        pipeline.build(run_params)

        assert pipeline._run_parameters is not None
