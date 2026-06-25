"""Tests for publish CLI command."""

from unittest.mock import MagicMock, patch

import pytest

from imap_mag.cli.publish import publish
from imap_mag.client.SDCDataAccess import SDCUploadError
from imap_mag.util.Environment import Environment


class TestPublishCli:
    def test_publish_succeeds_with_valid_file(
        self, dynamic_work_folder, clean_datastore, tmp_path
    ):
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

    def test_publish_raises_when_file_upload_fails(
        self, dynamic_work_folder, clean_datastore, tmp_path
    ):
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

    def test_publish_raises_when_file_not_found(
        self, dynamic_work_folder, clean_datastore, tmp_path
    ):
        nonexistent = tmp_path / "nonexistent.cdf"

        mock_sdc = MagicMock()

        with (
            patch("imap_mag.cli.publish.SDCDataAccess", return_value=mock_sdc),
            patch("imap_mag.cli.publish.initialiseLoggingForCommand"),
            Environment(MAG_DATA_STORE=str(tmp_path)),
        ):
            with pytest.raises(FileNotFoundError):
                publish([nonexistent])
