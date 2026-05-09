"""Tests for LoPivotPlatformPipeline."""

from datetime import datetime
from unittest.mock import MagicMock

from imap_mag.data_pipelines import FetchByDatesRunParameters
from imap_mag.data_pipelines.LoPivotPlatformPipeline import LoPivotPlatformPipeline


class TestLoPivotPlatformPipeline:
    def test_progress_item_id_is_lo_pivot_platform(self):
        assert LoPivotPlatformPipeline.PROGRESS_ITEM_ID == "LO_PIVOT_PLATFORM_ANGLE"

    def test_build_creates_stages(self):
        mock_settings = MagicMock()
        mock_settings.fetch_webtcad = MagicMock()
        mock_settings.setup_work_folder_for_command.return_value = MagicMock()

        pipeline = LoPivotPlatformPipeline(
            database=None,
            settings=mock_settings,
        )
        run_params = FetchByDatesRunParameters(
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 31),
        )
        pipeline.build(run_params)

        assert pipeline._run_parameters is not None
