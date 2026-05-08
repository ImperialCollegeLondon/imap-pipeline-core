"""Tests for WebPODA, IALiRTApiClient, and SDCDataAccess clients."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr


class TestWebPODAInit:
    def test_init_with_auth_code(self, tmp_path):
        from imap_mag.client.WebPODA import WebPODA

        poda = WebPODA(
            auth_code=SecretStr("test_key"),
            output_dir=tmp_path,
            webpoda_url="http://webpoda.example.com/",
        )
        assert poda is not None

    def test_init_with_none_auth_code(self, tmp_path):
        from imap_mag.client.WebPODA import WebPODA

        poda = WebPODA(
            auth_code=None,
            output_dir=tmp_path,
            webpoda_url="http://webpoda.example.com/",
        )
        assert poda is not None


class TestWebPODADownload:
    def _make_poda(self, tmp_path):
        from imap_mag.client.WebPODA import WebPODA

        return WebPODA(
            auth_code=SecretStr("test"),
            output_dir=tmp_path,
            webpoda_url="http://webpoda.example.com/",
        )

    def test_download_creates_output_file(self, tmp_path):
        poda = self._make_poda(tmp_path)
        mock_response = MagicMock()
        mock_response.content = b"binary_data"

        with patch("imap_mag.client.WebPODA.requests.get", return_value=mock_response):
            result = poda.download(
                packet="P_MAG_SID1",
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
            )

        assert result.exists()
        assert result.read_bytes() == b"binary_data"

    def test_download_creates_directory_if_not_exists(self, tmp_path):
        nested_dir = tmp_path / "subdir" / "nested"
        from imap_mag.client.WebPODA import WebPODA

        poda = WebPODA(
            auth_code=SecretStr("test"),
            output_dir=nested_dir,
            webpoda_url="http://webpoda.example.com/",
        )
        mock_response = MagicMock()
        mock_response.content = b"data"

        with patch("imap_mag.client.WebPODA.requests.get", return_value=mock_response):
            result = poda.download(
                packet="P_MAG_SID1",
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
            )

        assert nested_dir.exists()
        assert result.exists()

    def test_download_url_includes_packet_and_dates(self, tmp_path):
        poda = self._make_poda(tmp_path)
        mock_response = MagicMock()
        mock_response.content = b"data"
        captured_url = []

        def capture_request(url, headers):
            captured_url.append(url)
            return mock_response

        with patch("imap_mag.client.WebPODA.requests.get", side_effect=capture_request):
            poda.download(
                packet="P_MAG_SID1",
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
            )

        assert len(captured_url) == 1
        assert "P_MAG_SID1" in captured_url[0]
        assert "2025-01-01" in captured_url[0]
        assert "2025-01-02" in captured_url[0]

    def test_download_raises_on_http_error(self, tmp_path):
        import requests

        poda = self._make_poda(tmp_path)
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404")

        with patch("imap_mag.client.WebPODA.requests.get", return_value=mock_response):
            with pytest.raises(requests.exceptions.RequestException):
                poda.download(
                    packet="P_MAG_SID1",
                    start_date=datetime(2025, 1, 1),
                    end_date=datetime(2025, 1, 2),
                )

    def test_download_with_ert_mode(self, tmp_path):
        poda = self._make_poda(tmp_path)
        mock_response = MagicMock()
        mock_response.content = b"data"
        captured_url = []

        def capture_request(url, headers):
            captured_url.append(url)
            return mock_response

        with patch("imap_mag.client.WebPODA.requests.get", side_effect=capture_request):
            poda.download(
                packet="P_MAG_SID1",
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                ert=True,
            )

        assert "ert" in captured_url[0]


class TestWebPODAGetMaxErt:
    def _make_poda(self, tmp_path):
        from imap_mag.client.WebPODA import WebPODA

        return WebPODA(
            auth_code=SecretStr("test"),
            output_dir=tmp_path,
            webpoda_url="http://webpoda.example.com/",
        )

    def test_returns_none_when_no_data(self, tmp_path):
        poda = self._make_poda(tmp_path)
        mock_response = MagicMock()
        mock_response.content = b"ert\n"

        with patch("imap_mag.client.WebPODA.requests.get", return_value=mock_response):
            result = poda.get_max_ert(
                packet="P_MAG_SID1",
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
            )

        assert result is None

    def test_returns_max_datetime_from_response(self, tmp_path):
        poda = self._make_poda(tmp_path)
        mock_response = MagicMock()
        mock_response.content = (
            b"ert\n2025-01-01T10:00:00\n2025-01-01T12:00:00\n2025-01-01T11:00:00"
        )

        with patch("imap_mag.client.WebPODA.requests.get", return_value=mock_response):
            result = poda.get_max_ert(
                packet="P_MAG_SID1",
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
            )

        assert result == datetime(2025, 1, 1, 12, 0, 0)


class TestIALiRTApiClientInit:
    def test_init_sets_auth_code(self):
        import ialirt_data_access

        from imap_mag.client.IALiRTApiClient import IALiRTApiClient

        IALiRTApiClient(auth_code=SecretStr("my_key"))
        assert ialirt_data_access.config["API_KEY"] == "my_key"

    def test_init_sets_url(self):
        import ialirt_data_access

        from imap_mag.client.IALiRTApiClient import IALiRTApiClient

        IALiRTApiClient(auth_code=None, sdc_url="http://example.com/")
        assert ialirt_data_access.config["DATA_ACCESS_URL"] == "http://example.com/"

    def test_init_with_none_auth_code_does_not_set_key(self):
        import ialirt_data_access

        from imap_mag.client.IALiRTApiClient import IALiRTApiClient

        ialirt_data_access.config["API_KEY"] = "existing"
        IALiRTApiClient(auth_code=None, sdc_url=None)
        assert ialirt_data_access.config["API_KEY"] == "existing"


class TestIALiRTApiClientGetAllByDates:
    def _make_client(self):
        from imap_mag.client.IALiRTApiClient import IALiRTApiClient

        return IALiRTApiClient(auth_code=None, sdc_url=None)

    def test_returns_empty_when_start_equals_end(self):
        client = self._make_client()
        start = datetime(2025, 1, 1, 0, 0, 0)
        end = start + timedelta(seconds=3)

        result = client.get_all_by_dates(
            instrument="mag", start_date=start, end_date=end
        )
        assert result == []

    def test_returns_data_from_single_chunk(self):
        client = self._make_client()
        start = datetime(2025, 1, 1, 0, 0, 0)
        end = datetime(2025, 1, 1, 1, 0, 0)

        # Return data on first call, empty on second so loop terminates
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
        client = self._make_client()
        start = datetime(2025, 1, 1, 0, 0, 0)
        end = datetime(2025, 1, 1, 1, 0, 0)

        with patch("imap_mag.client.IALiRTApiClient.ialirt_data_access.data_product_query", return_value=[]):
            result = client.get_all_by_dates(
                instrument="mag", start_date=start, end_date=end
            )

        assert result == []

    def test_handles_dict_result_with_data_key(self):
        client = self._make_client()
        start = datetime(2025, 1, 1, 0, 0, 0)
        end = datetime(2025, 1, 1, 1, 0, 0)

        # Return dict on first call, empty list on second so loop terminates
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
        client = self._make_client()
        start = datetime(2025, 1, 1, 0, 0, 0)
        end = datetime(2025, 1, 1, 4, 0, 0)

        call_ranges = []

        def mock_query(instrument, time_utc_start, time_utc_end):
            call_ranges.append((time_utc_start, time_utc_end))
            return []

        with patch("imap_mag.client.IALiRTApiClient.ialirt_data_access.data_product_query", side_effect=mock_query):
            client.get_all_by_dates(
                instrument="mag",
                start_date=start,
                end_date=end,
                max_hours_per_chunk=2,
            )

        assert len(call_ranges) >= 2

    def test_pagination_advances_after_data_received(self):
        client = self._make_client()
        start = datetime(2025, 1, 1, 0, 0, 0)
        end = datetime(2025, 1, 1, 1, 0, 0)

        calls = [0]
        records_first = [{"time_utc": "2025-01-01T00:30:00", "value": 1.0}]

        def mock_query(instrument, time_utc_start, time_utc_end):
            calls[0] += 1
            if calls[0] == 1:
                return records_first
            return []

        with patch("imap_mag.client.IALiRTApiClient.ialirt_data_access.data_product_query", side_effect=mock_query):
            result = client.get_all_by_dates(
                instrument="mag", start_date=start, end_date=end
            )

        assert len(result) == 1


class TestSDCDataAccessInit:
    def test_init_sets_config(self, tmp_path):
        import imap_data_access

        from imap_mag.client.SDCDataAccess import SDCDataAccess

        SDCDataAccess(
            auth_code=SecretStr("auth"),
            data_dir=tmp_path,
            sdc_url="http://sdc.example.com/",
        )

        assert imap_data_access.config["API_KEY"] == "auth"
        assert imap_data_access.config["DATA_DIR"] == tmp_path

    def test_init_with_none_auth_code(self, tmp_path):
        import imap_data_access

        from imap_mag.client.SDCDataAccess import SDCDataAccess

        SDCDataAccess(
            auth_code=None,
            data_dir=tmp_path,
            sdc_url=None,
        )

        assert imap_data_access.config["API_KEY"] is None


class TestSDCDataAccessGetFilePath:
    def test_returns_filename_and_path(self, tmp_path):
        from imap_mag.client.SDCDataAccess import SDCDataAccess

        SDCDataAccess(auth_code=None, data_dir=tmp_path)
        filename, path = SDCDataAccess.get_file_path(
            level="l2",
            descriptor="norm-gse",
            start_date=datetime(2025, 1, 15),
            version="v001",
        )

        assert "20250115" in str(filename)
        assert "mag" in str(filename)


class TestSDCDataAccessUpload:
    def test_upload_calls_imap_data_access(self, tmp_path):
        import imap_data_access

        from imap_mag.client.SDCDataAccess import SDCDataAccess

        client = SDCDataAccess(auth_code=None, data_dir=tmp_path)

        with patch("imap_mag.client.SDCDataAccess.imap_data_access.upload") as mock_upload:
            client.upload("test_file.cdf")

        mock_upload.assert_called_once_with("test_file.cdf")

    def test_upload_raises_sdc_error_on_failure(self, tmp_path):
        import imap_data_access.io

        from imap_mag.client.SDCDataAccess import SDCDataAccess, SDCUploadError

        client = SDCDataAccess(auth_code=None, data_dir=tmp_path)

        with patch(
            "imap_mag.client.SDCDataAccess.imap_data_access.upload",
            side_effect=imap_data_access.io.IMAPDataAccessError("fail"),
        ):
            with pytest.raises(SDCUploadError, match="Failed to upload"):
                client.upload("test_file.cdf")
