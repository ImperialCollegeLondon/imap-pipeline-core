"""Unit tests for pollHK flow name generation."""

from datetime import datetime
from unittest.mock import patch

from imap_mag.util import HKPacket
from prefect_server.pollHK import generate_flow_run_name


class TestPollHKFlowGenerateName:
    def test_auto_run_includes_last_update(self):
        mock_params = {
            "hk_packets": list(HKPacket),
            "start_date": None,
            "end_date": None,
        }
        with patch("prefect_server.pollHK.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name()

        assert "last-update" in name
        assert "all-HK" in name

    def test_specific_dates_included_in_name(self):
        mock_params = {
            "hk_packets": [HKPacket.SID1],
            "start_date": datetime(2025, 6, 1),
            "end_date": datetime(2025, 6, 30),
        }
        with patch("prefect_server.pollHK.flow_run") as mock_flow_run:
            mock_flow_run.parameters = mock_params
            name = generate_flow_run_name()

        assert "01-06-2025" in name
