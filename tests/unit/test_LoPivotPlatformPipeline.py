"""Tests for LoPivotPlatformPipeline."""

from datetime import datetime

from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import FetchByDatesRunParameters
from imap_mag.data_pipelines.LoPivotPlatformPipeline import LoPivotPlatformPipeline


class TestLoPivotPlatformPipeline:
    def test_progress_item_id_is_lo_pivot_platform(self):
        assert LoPivotPlatformPipeline.PROGRESS_ITEM_ID == "LO_PIVOT_PLATFORM_ANGLE"

    def test_build_creates_stages(self):
        pipeline = LoPivotPlatformPipeline(
            database=None,
            settings=AppSettings(),  # type: ignore
        )
        run_params = FetchByDatesRunParameters(
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 31),
        )
        pipeline.build(run_params)

        assert pipeline._run_parameters is not None
