"""Tests for CLI fetch commands: binary, ialirt, science, spin_table."""

import asyncio
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from imap_mag.config import FetchMode
from imap_mag.util.Environment import Environment


class TestFetchBinary:
    def test_raises_when_neither_apid_nor_packet_provided(self, dynamic_work_folder):
        from imap_mag.cli.fetch.binary import fetch_binary

        with pytest.raises(ValueError, match="Must provide either"):
            fetch_binary(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                apid=None,
                packet=None,
            )

    def test_raises_when_both_apid_and_packet_provided(self, dynamic_work_folder):
        from imap_mag.cli.fetch.binary import fetch_binary
        from imap_mag.util import HKPacket

        with pytest.raises(ValueError, match="Must provide either"):
            fetch_binary(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                apid=1063,
                packet=HKPacket.SID1,
            )

    def test_downloads_using_apid(self, dynamic_work_folder, clean_datastore):
        from imap_mag.cli.fetch.binary import fetch_binary

        mock_poda = MagicMock()
        mock_fetch_binary = MagicMock()
        mock_fetch_binary.download_binaries.return_value = {}

        with (
            patch("imap_mag.cli.fetch.binary.WebPODA", return_value=mock_poda),
            patch("imap_mag.cli.fetch.binary.FetchBinary", return_value=mock_fetch_binary),
            patch("imap_mag.cli.fetch.binary.initialiseLoggingForCommand"),
        ):
            result = fetch_binary(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                apid=1063,
            )

        assert isinstance(result, dict)

    def test_downloads_using_packet_name(self, dynamic_work_folder, clean_datastore):
        from imap_mag.cli.fetch.binary import fetch_binary
        from imap_mag.util import HKPacket

        mock_poda = MagicMock()
        mock_fetch_binary = MagicMock()
        mock_fetch_binary.download_binaries.return_value = {}

        with (
            patch("imap_mag.cli.fetch.binary.WebPODA", return_value=mock_poda),
            patch("imap_mag.cli.fetch.binary.FetchBinary", return_value=mock_fetch_binary),
            patch("imap_mag.cli.fetch.binary.initialiseLoggingForCommand"),
        ):
            result = fetch_binary(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
                packet=HKPacket.SID1,
            )

        assert isinstance(result, dict)

    def test_publishes_to_datastore_when_configured(self, dynamic_work_folder, clean_datastore):
        from imap_mag.cli.fetch.binary import fetch_binary
        from imap_mag.io.file.HKBinaryPathHandler import HKBinaryPathHandler

        mock_file = Path("/tmp/test.pkts")
        mock_handler = MagicMock(spec=HKBinaryPathHandler)
        mock_poda = MagicMock()
        mock_fetch_binary = MagicMock()
        mock_fetch_binary.download_binaries.return_value = {mock_file: mock_handler}

        mock_datastore = MagicMock()
        mock_datastore.add_file.return_value = (mock_file, mock_handler)

        with (
            patch("imap_mag.cli.fetch.binary.WebPODA", return_value=mock_poda),
            patch("imap_mag.cli.fetch.binary.FetchBinary", return_value=mock_fetch_binary),
            patch("imap_mag.cli.fetch.binary.DatastoreFileManager.CreateByMode", return_value=mock_datastore),
            patch("imap_mag.cli.fetch.binary.initialiseLoggingForCommand"),
        ):
            with Environment(MAG_FETCH_BINARY_PUBLISH_TO_DATA_STORE="true"):
                result = fetch_binary(
                    start_date=datetime(2025, 1, 1),
                    end_date=datetime(2025, 1, 2),
                    apid=1063,
                )

        assert isinstance(result, dict)


class TestFetchIalirt:
    def test_fetch_ialirt_returns_empty_when_no_data(self, dynamic_work_folder, clean_datastore):
        from imap_mag.cli.fetch.ialirt import fetch_ialirt

        mock_ialirt_client = MagicMock()
        mock_fetch = MagicMock()
        mock_fetch.download_mag_to_csv.return_value = {}

        with (
            patch("imap_mag.cli.fetch.ialirt.IALiRTApiClient", return_value=mock_ialirt_client),
            patch("imap_mag.cli.fetch.ialirt.FetchIALiRT", return_value=mock_fetch),
            patch("imap_mag.cli.fetch.ialirt.initialiseLoggingForCommand"),
        ):
            result = fetch_ialirt(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
            )

        assert result == {}

    def test_fetch_ialirt_hk_returns_empty_when_no_data(self, dynamic_work_folder, clean_datastore):
        from imap_mag.cli.fetch.ialirt import fetch_ialirt_hk

        mock_ialirt_client = MagicMock()
        mock_fetch = MagicMock()
        mock_fetch.download_mag_hk_to_csv.return_value = {}

        with (
            patch("imap_mag.cli.fetch.ialirt.IALiRTApiClient", return_value=mock_ialirt_client),
            patch("imap_mag.cli.fetch.ialirt.FetchIALiRT", return_value=mock_fetch),
            patch("imap_mag.cli.fetch.ialirt.initialiseLoggingForCommand"),
        ):
            result = fetch_ialirt_hk(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
            )

        assert result == {}

    def test_fetch_ialirt_publishes_to_datastore(self, dynamic_work_folder, clean_datastore):
        from imap_mag.cli.fetch.ialirt import fetch_ialirt
        from imap_mag.io.file.IALiRTPathHandler import IALiRTPathHandler

        mock_file = Path("/tmp/test.csv")
        mock_handler = MagicMock(spec=IALiRTPathHandler)
        mock_ialirt_client = MagicMock()
        mock_fetch = MagicMock()
        mock_fetch.download_mag_to_csv.return_value = {mock_file: mock_handler}

        mock_datastore = MagicMock()
        mock_datastore.add_file.return_value = (mock_file, mock_handler)

        with (
            patch("imap_mag.cli.fetch.ialirt.IALiRTApiClient", return_value=mock_ialirt_client),
            patch("imap_mag.cli.fetch.ialirt.FetchIALiRT", return_value=mock_fetch),
            patch("imap_mag.cli.fetch.ialirt.DatastoreFileManager.CreateByMode", return_value=mock_datastore),
            patch("imap_mag.cli.fetch.ialirt.initialiseLoggingForCommand"),
        ):
            with Environment(MAG_FETCH_IALIRT_PUBLISH_TO_DATA_STORE="true"):
                result = fetch_ialirt(
                    start_date=datetime(2025, 1, 1),
                    end_date=datetime(2025, 1, 2),
                )

        assert isinstance(result, dict)


class TestFetchScience:
    def test_fetch_science_returns_empty_when_no_data(self, dynamic_work_folder, clean_datastore):
        from imap_mag.cli.fetch.science import fetch_science

        mock_sdc = MagicMock()
        mock_fetch_science = MagicMock()
        mock_fetch_science.download_science.return_value = {}

        with (
            patch("imap_mag.cli.fetch.science.SDCDataAccess", return_value=mock_sdc),
            patch("imap_mag.cli.fetch.science.FetchScience", return_value=mock_fetch_science),
            patch("imap_mag.cli.fetch.science.initialiseLoggingForCommand"),
        ):
            result = fetch_science(
                start_date=datetime(2025, 1, 1),
                end_date=datetime(2025, 1, 2),
            )

        assert result == {}

    def test_fetch_science_publishes_when_configured(self, dynamic_work_folder, clean_datastore):
        from imap_mag.cli.fetch.science import fetch_science
        from imap_mag.io.file.SciencePathHandler import SciencePathHandler

        mock_file = Path("/tmp/science.cdf")
        mock_handler = MagicMock(spec=SciencePathHandler)
        mock_sdc = MagicMock()
        mock_fetch_science = MagicMock()
        mock_fetch_science.download_science.return_value = {mock_file: mock_handler}

        mock_datastore = MagicMock()
        mock_datastore.add_file.return_value = (mock_file, mock_handler)

        with (
            patch("imap_mag.cli.fetch.science.SDCDataAccess", return_value=mock_sdc),
            patch("imap_mag.cli.fetch.science.FetchScience", return_value=mock_fetch_science),
            patch("imap_mag.cli.fetch.science.DatastoreFileManager.CreateByMode", return_value=mock_datastore),
            patch("imap_mag.cli.fetch.science.initialiseLoggingForCommand"),
        ):
            with Environment(MAG_FETCH_SCIENCE_PUBLISH_TO_DATA_STORE="true"):
                result = fetch_science(
                    start_date=datetime(2025, 1, 1),
                    end_date=datetime(2025, 1, 2),
                )

        assert isinstance(result, dict)


class TestPublishCli:
    def test_publish_succeeds_with_valid_file(self, dynamic_work_folder, clean_datastore, tmp_path):
        from imap_mag.cli.publish import publish

        test_file = tmp_path / "imap_mag_l2-calibration_20251017_v001.cdf"
        test_file.write_text("fake cdf content")

        mock_sdc = MagicMock()

        with (
            patch("imap_mag.cli.publish.SDCDataAccess", return_value=mock_sdc),
            patch("imap_mag.cli.publish.initialiseLoggingForCommand"),
            Environment(MAG_DATA_STORE=str(tmp_path)),
        ):
            publish([test_file])

        mock_sdc.upload.assert_called_once()

    def test_publish_raises_when_file_upload_fails(self, dynamic_work_folder, clean_datastore, tmp_path):
        from imap_mag.cli.publish import publish
        from imap_mag.client.SDCDataAccess import SDCUploadError

        test_file = tmp_path / "imap_mag_l2-calibration_20251017_v001.cdf"
        test_file.write_text("fake cdf content")

        mock_sdc = MagicMock()
        mock_sdc.upload.side_effect = SDCUploadError("upload failed")

        with (
            patch("imap_mag.cli.publish.SDCDataAccess", return_value=mock_sdc),
            patch("imap_mag.cli.publish.initialiseLoggingForCommand"),
            Environment(MAG_DATA_STORE=str(tmp_path)),
        ):
            with pytest.raises(RuntimeError, match="Failed to publish"):
                publish([test_file])

    def test_publish_raises_when_file_not_found(self, dynamic_work_folder, clean_datastore, tmp_path):
        from imap_mag.cli.publish import publish

        nonexistent = tmp_path / "nonexistent.cdf"

        mock_sdc = MagicMock()

        with (
            patch("imap_mag.cli.publish.SDCDataAccess", return_value=mock_sdc),
            patch("imap_mag.cli.publish.initialiseLoggingForCommand"),
            Environment(MAG_DATA_STORE=str(tmp_path)),
        ):
            with pytest.raises(FileNotFoundError):
                publish([nonexistent])
