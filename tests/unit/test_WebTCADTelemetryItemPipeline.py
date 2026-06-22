"""Tests for WebTCADTelemetryItemPipeline."""

from datetime import datetime

from imap_mag.client.WebTCADLaTiS import HKWebTCADItems
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import FetchByDatesRunParameters
from imap_mag.data_pipelines.WebTCADTelemetryItemPipeline import (
    WebTCADTelemetryItemPipeline,
)

ITEM = HKWebTCADItems.LO_PIVOT_PLATFORM_ANGLE


class TestWebTCADTelemetryItemPipeline:
    def test_progress_item_id_uses_item_name(self):
        pipeline = WebTCADTelemetryItemPipeline(
            item=ITEM,
            database=None,
            settings=AppSettings(),  # type: ignore
        )
        assert pipeline.progress_item_id == ITEM.name

    def test_build_creates_stages(self):
        pipeline = WebTCADTelemetryItemPipeline(
            item=ITEM,
            database=None,
            settings=AppSettings(),  # type: ignore
        )
        run_params = FetchByDatesRunParameters(
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 31),
        )
        pipeline.build(run_params)

        assert pipeline._run_parameters is not None
