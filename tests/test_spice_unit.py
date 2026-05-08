"""Unit tests for fetch spice module helper functions."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from imap_mag.cli.fetch.spice import (
    _file_meta_dates_within_range,
    download_spice_files_later_than,
    fetch_spice,
    publish_spice_kernel,
)


def _make_file_with_meta(file_meta=None):
    f = MagicMock()
    f.file_meta = file_meta
    f.name = "test_file"
    f.id = 1
    return f


class TestFilemetaDatesWithinRange:
    def test_returns_true_when_file_meta_is_none(self):
        f = _make_file_with_meta(file_meta=None)
        result = _file_meta_dates_within_range(
            f, datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 12, 31, tzinfo=UTC)
        )
        assert result is True

    def test_returns_true_when_max_date_not_in_metadata(self):
        f = _make_file_with_meta(file_meta={"kernel_type": "attitude_history"})
        result = _file_meta_dates_within_range(
            f, datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 12, 31, tzinfo=UTC)
        )
        assert result is True

    def test_returns_false_and_warns_when_min_date_not_parseable(self):
        f = _make_file_with_meta(
            file_meta={"max_date_datetime": "2025-06-01, 00:00:00"}
            # "min_date_datetime" absent → try_extract returns None → warning
        )
        result = _file_meta_dates_within_range(
            f, datetime(2025, 1, 1), datetime(2025, 12, 31)
        )
        assert result is False

    def test_returns_false_when_file_dates_entirely_before_range(self):
        f = _make_file_with_meta(
            file_meta={
                "min_date_datetime": "2024-01-01, 00:00:00",
                "max_date_datetime": "2024-06-01, 00:00:00",
            }
        )
        result = _file_meta_dates_within_range(
            f, datetime(2025, 1, 1), datetime(2025, 12, 31)
        )
        assert result is False


class TestDownloadSpiceFilesLaterThan:
    def _make_data_access(self, file_size=1024):
        mock_da = MagicMock()
        mock_path = MagicMock(spec=Path)
        mock_path.stat.return_value.st_size = file_size
        mock_da.download.return_value = mock_path
        return mock_da, mock_path

    def test_returns_empty_dict_for_empty_query_results(self):
        mock_da, _ = self._make_data_access()
        result = download_spice_files_later_than(mock_da, None, [])
        assert result == {}

    def test_skips_file_with_none_filename(self):
        mock_da, _ = self._make_data_access()
        result = download_spice_files_later_than(
            mock_da, None, [{"file_name": None, "ingestion_date": "2025-01-01, 00:00:00"}]
        )
        mock_da.download.assert_not_called()
        assert result == {}

    def test_skips_file_ingested_before_start_day(self):
        mock_da, _ = self._make_data_access()
        file_meta = {"file_name": "test.bsp", "ingestion_date": "2025-01-01, 00:00:00"}
        result = download_spice_files_later_than(
            mock_da, datetime(2025, 1, 2), [file_meta]
        )
        mock_da.download.assert_not_called()
        assert result == {}

    def test_downloads_file_and_returns_in_dict(self):
        mock_da, mock_path = self._make_data_access(file_size=2048)
        file_meta = {"file_name": "test.bsp", "ingestion_date": "2025-01-10, 00:00:00"}
        result = download_spice_files_later_than(
            mock_da, datetime(2025, 1, 2), [file_meta]
        )
        mock_da.download.assert_called_once_with("test.bsp")
        assert mock_path in result

    def test_logs_warning_and_excludes_empty_file(self):
        mock_da, _ = self._make_data_access(file_size=0)
        file_meta = {"file_name": "empty.bsp", "ingestion_date": "2025-01-10, 00:00:00"}
        result = download_spice_files_later_than(
            mock_da, None, [file_meta]
        )
        mock_da.download.assert_called_once()
        assert result == {}


def _make_mock_fetch_spice_settings(tmp_path):
    mock_settings = MagicMock()
    mock_settings.setup_work_folder_for_command.return_value = tmp_path
    return mock_settings


class TestFetchSpice:
    def test_returns_empty_list_when_no_query_results(self, tmp_path):
        with (
            patch(
                "imap_mag.cli.fetch.spice.AppSettings",
                return_value=_make_mock_fetch_spice_settings(tmp_path),
            ),
            patch("imap_mag.cli.fetch.spice.initialiseLoggingForCommand"),
            patch("imap_mag.cli.fetch.spice.SDCDataAccess") as mock_sdc_cls,
        ):
            mock_sdc_cls.return_value.spice_query.return_value = []
            result = fetch_spice()
        assert result == []

    def test_returns_file_tuples_when_handler_found_and_not_publishing(self, tmp_path):
        mock_file_path = Path("spice/ck/test.bc")
        mock_handler = MagicMock()

        with (
            patch(
                "imap_mag.cli.fetch.spice.AppSettings",
                return_value=_make_mock_fetch_spice_settings(tmp_path),
            ),
            patch("imap_mag.cli.fetch.spice.initialiseLoggingForCommand"),
            patch("imap_mag.cli.fetch.spice.SDCDataAccess") as mock_sdc_cls,
            patch(
                "imap_mag.cli.fetch.spice.download_spice_files_later_than",
                return_value={mock_file_path: {"file_name": "test.bc"}},
            ),
            patch(
                "imap_mag.cli.fetch.spice.SPICEPathHandler.from_filename",
                return_value=mock_handler,
            ),
        ):
            mock_settings = _make_mock_fetch_spice_settings(tmp_path)
            mock_settings.fetch_spice.publish_to_data_store = False
            mock_sdc_cls.return_value.spice_query.return_value = [{"file_name": "test.bc"}]
            with patch(
                "imap_mag.cli.fetch.spice.AppSettings",
                return_value=mock_settings,
            ):
                result = fetch_spice()

        assert len(result) == 1
        assert result[0][0] == mock_file_path
        assert result[0][1] == mock_handler

    def test_skips_file_when_handler_returns_none(self, tmp_path):
        mock_file_path = Path("spice/ck/unparseable.bc")
        mock_settings = _make_mock_fetch_spice_settings(tmp_path)
        mock_settings.fetch_spice.publish_to_data_store = False

        with (
            patch("imap_mag.cli.fetch.spice.AppSettings", return_value=mock_settings),
            patch("imap_mag.cli.fetch.spice.initialiseLoggingForCommand"),
            patch("imap_mag.cli.fetch.spice.SDCDataAccess") as mock_sdc_cls,
            patch(
                "imap_mag.cli.fetch.spice.download_spice_files_later_than",
                return_value={mock_file_path: {"file_name": "unparseable.bc"}},
            ),
            patch(
                "imap_mag.cli.fetch.spice.SPICEPathHandler.from_filename",
                return_value=None,
            ),
        ):
            mock_sdc_cls.return_value.spice_query.return_value = [
                {"file_name": "unparseable.bc"}
            ]
            result = fetch_spice()

        assert result == []

    def test_uses_output_manager_when_publish_to_data_store_is_true(self, tmp_path):
        mock_file_path = Path("spice/ck/test.bc")
        mock_output_file = Path("datastore/spice/ck/test.bc")
        mock_output_handler = MagicMock()
        mock_output_manager = MagicMock()
        mock_output_manager.add_file.return_value = (mock_output_file, mock_output_handler)
        mock_settings = _make_mock_fetch_spice_settings(tmp_path)
        mock_settings.fetch_spice.publish_to_data_store = True

        with (
            patch("imap_mag.cli.fetch.spice.AppSettings", return_value=mock_settings),
            patch("imap_mag.cli.fetch.spice.initialiseLoggingForCommand"),
            patch("imap_mag.cli.fetch.spice.SDCDataAccess") as mock_sdc_cls,
            patch(
                "imap_mag.cli.fetch.spice.download_spice_files_later_than",
                return_value={mock_file_path: {"file_name": "test.bc"}},
            ),
            patch(
                "imap_mag.cli.fetch.spice.SPICEPathHandler.from_filename",
                return_value=MagicMock(),
            ),
            patch(
                "imap_mag.cli.fetch.spice.DatastoreFileManager.CreateByMode",
                return_value=mock_output_manager,
            ),
        ):
            mock_sdc_cls.return_value.spice_query.return_value = [{"file_name": "test.bc"}]
            result = fetch_spice()

        assert len(result) == 1
        assert result[0][0] == mock_output_file


class TestPublishSpiceKernel:
    def test_raises_when_handler_cannot_parse_kernel_filename(self, tmp_path):
        mock_settings = MagicMock()
        kernel_path = tmp_path / "unparseable.tm"
        kernel_path.write_text("kernel content")

        with patch(
            "imap_mag.cli.fetch.spice.SPICEPathHandler.from_filename",
            return_value=None,
        ):
            with pytest.raises(RuntimeError, match="could not be parsed"):
                publish_spice_kernel(mock_settings, kernel_path, use_database=False)

    def test_returns_output_file_path_when_publish_succeeds(self, tmp_path):
        mock_settings = MagicMock()
        kernel_path = tmp_path / "imap_sclk_0001.tsc"
        kernel_path.write_text("kernel content")
        output_file = tmp_path / "datastore" / "spice" / "sclk" / "imap_sclk_0001.tsc"
        mock_output_manager = MagicMock()
        mock_output_manager.add_file.return_value = (output_file, MagicMock())

        with (
            patch(
                "imap_mag.cli.fetch.spice.SPICEPathHandler.from_filename",
                return_value=MagicMock(),
            ),
            patch(
                "imap_mag.cli.fetch.spice.DatastoreFileManager.CreateByMode",
                return_value=mock_output_manager,
            ),
        ):
            result = publish_spice_kernel(mock_settings, kernel_path, use_database=False)

        assert result == output_file
