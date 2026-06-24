"""Tests for WebPODA client."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import SecretStr

from imap_mag.client.WebPODA import WebPODA


def _make_poda(tmp_path):
    return WebPODA(
        auth_code=SecretStr("test"),
        output_dir=tmp_path,
        webpoda_url="http://webpoda.example.com/",
    )


class TestWebPODAInit:
    def test_init_with_auth_code(self, tmp_path):
        poda = WebPODA(
            auth_code=SecretStr("test_key"),
            output_dir=tmp_path,
            webpoda_url="http://webpoda.example.com/",
        )
        assert poda is not None

    def test_init_with_none_auth_code(self, tmp_path):
        poda = WebPODA(
            auth_code=None,
            output_dir=tmp_path,
            webpoda_url="http://webpoda.example.com/",
        )
        assert poda is not None


class TestWebPODADownload:
    def test_download_creates_output_file(self, tmp_path):
        poda = _make_poda(tmp_path)
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
        poda = _make_poda(tmp_path)
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
        poda = _make_poda(tmp_path)
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "404"
        )

        with patch("imap_mag.client.WebPODA.requests.get", return_value=mock_response):
            with pytest.raises(requests.exceptions.RequestException):
                poda.download(
                    packet="P_MAG_SID1",
                    start_date=datetime(2025, 1, 1),
                    end_date=datetime(2025, 1, 2),
                )

    def test_download_with_ert_mode(self, tmp_path):
        poda = _make_poda(tmp_path)
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
    def test_returns_none_when_no_data(self, tmp_path):
        poda = _make_poda(tmp_path)
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
        poda = _make_poda(tmp_path)
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
