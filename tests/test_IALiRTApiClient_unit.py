"""Unit tests for IALiRTApiClient.get_all_by_dates."""

from datetime import datetime, timedelta
from unittest.mock import patch

from imap_mag.client.IALiRTApiClient import IALiRTApiClient


def _make_client():
    return IALiRTApiClient(auth_code=None, sdc_url=None)


class TestIALiRTApiClientGetAllByDates:
    def test_returns_empty_when_start_equals_end(self):
        client = _make_client()
        start = datetime(2025, 1, 1, 0, 0, 0)
        end = start + timedelta(seconds=3)

        result = client.get_all_by_dates(
            instrument="mag", start_date=start, end_date=end
        )
        assert result == []

    def test_returns_data_from_single_chunk(self):
        client = _make_client()
        start = datetime(2025, 1, 1, 0, 0, 0)
        end = datetime(2025, 1, 1, 1, 0, 0)

        side_effects = [
            [{"time_utc": "2025-01-01T00:30:00", "value": 1.0}],
            [],
        ]

        with patch(
            "imap_mag.client.IALiRTApiClient.ialirt_data_access.data_product_query",
            side_effect=side_effects,
        ):
            result = client.get_all_by_dates(
                instrument="mag", start_date=start, end_date=end
            )

        assert len(result) == 1
        assert result[0]["value"] == 1.0

    def test_stops_when_no_more_data(self):
        client = _make_client()
        start = datetime(2025, 1, 1, 0, 0, 0)
        end = datetime(2025, 1, 1, 1, 0, 0)

        with patch(
            "imap_mag.client.IALiRTApiClient.ialirt_data_access.data_product_query",
            return_value=[],
        ):
            result = client.get_all_by_dates(
                instrument="mag", start_date=start, end_date=end
            )

        assert result == []

    def test_handles_dict_result_with_data_key(self):
        client = _make_client()
        start = datetime(2025, 1, 1, 0, 0, 0)
        end = datetime(2025, 1, 1, 1, 0, 0)

        side_effects = [
            {"data": [{"time_utc": "2025-01-01T00:30:00", "value": 2.0}]},
            [],
        ]

        with patch(
            "imap_mag.client.IALiRTApiClient.ialirt_data_access.data_product_query",
            side_effect=side_effects,
        ):
            result = client.get_all_by_dates(
                instrument="mag", start_date=start, end_date=end
            )

        assert len(result) == 1

    def test_chunks_by_max_hours(self):
        client = _make_client()
        start = datetime(2025, 1, 1, 0, 0, 0)
        end = datetime(2025, 1, 1, 4, 0, 0)

        call_ranges = []

        def mock_query(instrument, time_utc_start, time_utc_end):
            call_ranges.append((time_utc_start, time_utc_end))
            return []

        with patch(
            "imap_mag.client.IALiRTApiClient.ialirt_data_access.data_product_query",
            side_effect=mock_query,
        ):
            client.get_all_by_dates(
                instrument="mag",
                start_date=start,
                end_date=end,
                max_hours_per_chunk=2,
            )

        assert len(call_ranges) >= 2

    def test_pagination_advances_after_data_received(self):
        client = _make_client()
        start = datetime(2025, 1, 1, 0, 0, 0)
        end = datetime(2025, 1, 1, 1, 0, 0)

        calls = [0]
        records_first = [{"time_utc": "2025-01-01T00:30:00", "value": 1.0}]

        def mock_query(instrument, time_utc_start, time_utc_end):
            calls[0] += 1
            if calls[0] == 1:
                return records_first
            return []

        with patch(
            "imap_mag.client.IALiRTApiClient.ialirt_data_access.data_product_query",
            side_effect=mock_query,
        ):
            result = client.get_all_by_dates(
                instrument="mag", start_date=start, end_date=end
            )

        assert len(result) == 1
