"""Tests for SpinTablePipeline."""

from datetime import datetime
from unittest.mock import MagicMock

from imap_mag.data_pipelines import FetchByDatesRunParameters
from imap_mag.data_pipelines.SpinTablePipeline import SpinTablePipeline


class TestSpinTablePipeline:
    def test_progress_item_id_is_spin_table(self):
        assert SpinTablePipeline.PROGRESS_ITEM_ID == "SPIN_TABLE"

    def test_build_creates_stages(self):
        mock_settings = MagicMock()
        mock_settings.fetch_spice = MagicMock()
        mock_settings.setup_work_folder_for_command.return_value = MagicMock()
        mock_client = MagicMock()

        pipeline = SpinTablePipeline(
            database=None,
            settings=mock_settings,
            client=mock_client,
        )
        run_params = FetchByDatesRunParameters(
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 31),
        )
        pipeline.build(run_params)

        assert pipeline._run_parameters is not None
