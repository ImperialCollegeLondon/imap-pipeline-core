"""Unit tests for the plot_ialirt CLI command."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from imap_mag.cli.plot.plot_ialirt import plot_ialirt
from imap_mag.config import SaveMode
from imap_mag.io.file import IALiRTQuicklookPathHandler
from imap_mag.plot.plot_ialirt_files import (
    _load_csv_files,
    _merge_science_and_hk,
    create_figure,
    plot_ialirt_files,
    set_time_format,
)

_IALIRT_COLUMNS = [
    "mag_B_GSE_x",
    "mag_B_GSE_y",
    "mag_B_GSE_z",
    "mag_B_magnitude",
    "mag_hk_hk1v5_danger",
    "mag_hk_hk1v5c_danger",
    "mag_hk_hk1v8_danger",
    "mag_hk_hk1v8c_danger",
    "mag_hk_hk2v5_danger",
    "mag_hk_hk2v5c_danger",
    "mag_hk_hkp8v5_danger",
    "mag_hk_hkp8v5c_danger",
    "mag_hk_hk1v5_warn",
    "mag_hk_hk1v5c_warn",
    "mag_hk_hk1v8_warn",
    "mag_hk_hk1v8c_warn",
    "mag_hk_hk2v5_warn",
    "mag_hk_hk2v5c_warn",
    "mag_hk_hkp8v5_warn",
    "mag_hk_hkp8v5c_warn",
    "mag_hk_hk3v3",
    "mag_hk_hk3v3_current",
    "mag_hk_hkn8v5",
    "mag_hk_hkn8v5_current",
    "mag_hk_multbit_errs",
    "mag_hk_mode",
    "mag_hk_fob_saturated",
    "mag_hk_fib_saturated",
    "mag_hk_fob_range",
    "mag_hk_fib_range",
    "mag_hk_pri_isvalid",
    "mag_hk_sec_isvalid",
    "mag_hk_fob_temp",
    "mag_hk_fib_temp",
    "mag_hk_icu_temp",
]


def _make_ialirt_df(n_rows=3):
    index = pd.date_range("2025-10-17", periods=n_rows, freq="1min", tz="UTC")
    return pd.DataFrame(
        {col: [float(i) for i in range(n_rows)] for col in _IALIRT_COLUMNS},
        index=index,
    )


def _make_mock_settings(tmp_path: Path, publish_to_data_store: bool = False):
    mock_settings = MagicMock()
    mock_settings.setup_work_folder_for_command.return_value = tmp_path
    mock_settings.plot_ialirt.publish_to_data_store = publish_to_data_store
    mock_settings.data_store = tmp_path
    return mock_settings


class TestPlotIALiRTNoFiles:
    def test_returns_empty_dict_when_no_science_or_hk_files_found(self, tmp_path):
        mock_settings = _make_mock_settings(tmp_path)

        with (
            patch(
                "imap_mag.cli.plot.plot_ialirt.AppSettings", return_value=mock_settings
            ),
            patch("imap_mag.cli.plot.plot_ialirt.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_files_for_work",
                return_value=[],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_hk_files_for_work",
                return_value=[],
            ),
        ):
            result = plot_ialirt()

        assert result == {}


class TestPlotIALiRTLocalOnly:
    def test_returns_generated_figure_when_not_publishing_to_datastore(self, tmp_path):
        mock_settings = _make_mock_settings(tmp_path, publish_to_data_store=False)

        fake_science_file = tmp_path / "science.csv"
        fake_science_file.write_text("data")
        mock_handler = MagicMock(spec=IALiRTQuicklookPathHandler)
        mock_handler.content_date = None

        with (
            patch(
                "imap_mag.cli.plot.plot_ialirt.AppSettings", return_value=mock_settings
            ),
            patch("imap_mag.cli.plot.plot_ialirt.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_files_for_work",
                return_value=[fake_science_file],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_hk_files_for_work",
                return_value=[],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.plot_ialirt_files",
                return_value={fake_science_file: mock_handler},
            ),
        ):
            result = plot_ialirt(save_mode=SaveMode.LocalOnly)

        assert result == {fake_science_file: mock_handler}

    def test_logs_info_message_with_file_count(self, tmp_path):
        mock_settings = _make_mock_settings(tmp_path, publish_to_data_store=False)

        fake_hk_file = tmp_path / "hk.csv"
        fake_hk_file.write_text("data")
        mock_handler = MagicMock(spec=IALiRTQuicklookPathHandler)
        mock_handler.content_date = None

        with (
            patch(
                "imap_mag.cli.plot.plot_ialirt.AppSettings", return_value=mock_settings
            ),
            patch("imap_mag.cli.plot.plot_ialirt.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_files_for_work",
                return_value=[],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_hk_files_for_work",
                return_value=[fake_hk_file],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.plot_ialirt_files",
                return_value={fake_hk_file: mock_handler},
            ),
        ):
            result = plot_ialirt(save_mode=SaveMode.LocalOnly)

        assert fake_hk_file in result


class TestPlotIALiRTPublishToDatastore:
    def test_publishes_generated_figure_to_datastore(self, tmp_path):
        mock_settings = _make_mock_settings(tmp_path, publish_to_data_store=True)

        fake_file = tmp_path / "plot.png"
        fake_file.write_text("data")
        mock_handler = MagicMock(spec=IALiRTQuicklookPathHandler)
        mock_handler.content_date = None

        output_file = tmp_path / "output.png"
        output_handler = MagicMock(spec=IALiRTQuicklookPathHandler)
        output_handler.content_date = None

        mock_datastore = MagicMock()
        mock_datastore.add_file.return_value = (output_file, output_handler)

        with (
            patch(
                "imap_mag.cli.plot.plot_ialirt.AppSettings", return_value=mock_settings
            ),
            patch("imap_mag.cli.plot.plot_ialirt.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_files_for_work",
                return_value=[fake_file],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_hk_files_for_work",
                return_value=[],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.plot_ialirt_files",
                return_value={fake_file: mock_handler},
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.DatastoreFileManager.CreateByMode",
                return_value=mock_datastore,
            ),
        ):
            result = plot_ialirt(save_mode=SaveMode.LocalAndDatabase)

        assert output_file in result
        mock_datastore.add_file.assert_called()

    def test_adds_latest_copy_when_content_date_is_today(self, tmp_path):
        mock_settings = _make_mock_settings(tmp_path, publish_to_data_store=True)
        today = datetime(2025, 10, 21, 0, 0, 0)

        fake_file = tmp_path / "plot.png"
        fake_file.write_text("data")
        mock_handler = MagicMock(spec=IALiRTQuicklookPathHandler)
        mock_handler.content_date = today
        mock_handler.root_folder = "ialirt"
        mock_handler.get_plot_type.return_value = "quicklook"

        output_file = tmp_path / "output.png"
        output_handler = MagicMock(spec=IALiRTQuicklookPathHandler)
        output_handler.content_date = today
        output_handler.root_folder = "ialirt"
        output_handler.get_plot_type.return_value = "quicklook"

        mock_datastore = MagicMock()
        mock_datastore.add_file.return_value = (output_file, output_handler)

        with (
            patch(
                "imap_mag.cli.plot.plot_ialirt.AppSettings", return_value=mock_settings
            ),
            patch("imap_mag.cli.plot.plot_ialirt.initialiseLoggingForCommand"),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_files_for_work",
                return_value=[fake_file],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.fetch_ialirt_hk_files_for_work",
                return_value=[],
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.plot_ialirt_files",
                return_value={fake_file: mock_handler},
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.DatastoreFileManager.CreateByMode",
                return_value=mock_datastore,
            ),
            patch(
                "imap_mag.cli.plot.plot_ialirt.DatetimeProvider.today",
                return_value=today,
            ),
        ):
            result = plot_ialirt(save_mode=SaveMode.LocalAndDatabase)

        assert mock_datastore.add_file.call_count == 2


class TestMergeScineceAndHkData:
    def test_returns_empty_dataframe_when_both_inputs_empty(self):
        result = _merge_science_and_hk(pd.DataFrame(), pd.DataFrame())
        assert result.empty

    def test_returns_hk_when_science_is_empty(self):
        hk = pd.DataFrame({"col": [1, 2]})
        result = _merge_science_and_hk(pd.DataFrame(), hk)
        assert list(result["col"]) == [1, 2]

    def test_returns_science_when_hk_is_empty(self):
        science = pd.DataFrame({"col": [3, 4]})
        result = _merge_science_and_hk(science, pd.DataFrame())
        assert list(result["col"]) == [3, 4]

    def test_outer_joins_science_and_hk_on_index(self):
        idx = pd.date_range("2025-01-01", periods=2, freq="1min")
        science = pd.DataFrame({"sci": [1.0, 2.0]}, index=idx)
        hk = pd.DataFrame({"hk": [10.0, 20.0]}, index=idx)
        result = _merge_science_and_hk(science, hk)
        assert "sci" in result.columns
        assert "hk" in result.columns


class TestLoadCsvFiles:
    def test_returns_empty_dataframe_for_empty_list(self):
        result = _load_csv_files([])
        assert result.empty

    def test_loads_single_csv_file(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("time_utc,value\n2025-01-01 00:00:00,42.0\n")
        result = _load_csv_files([csv_file])
        assert len(result) == 1
        assert result["value"].iloc[0] == 42.0

    def test_concatenates_multiple_csv_files(self, tmp_path):
        f1 = tmp_path / "a.csv"
        f1.write_text("time_utc,value\n2025-01-01 00:00:00,1.0\n")
        f2 = tmp_path / "b.csv"
        f2.write_text("time_utc,value\n2025-01-02 00:00:00,2.0\n")
        result = _load_csv_files([f1, f2])
        assert len(result) == 2


class TestPlotIalirtFilesFunction:
    def test_returns_empty_when_no_input_files(self, tmp_path):
        result = plot_ialirt_files(
            science_files=[],
            hk_files=[],
            save_folder=tmp_path,
        )
        assert result == {}

    def test_calls_create_figure_once_when_combine_plots(self, tmp_path):
        df = _make_ialirt_df()
        mock_handler = MagicMock()

        with (
            patch(
                "imap_mag.plot.plot_ialirt_files._load_csv_files",
                side_effect=[df, pd.DataFrame()],
            ),
            patch(
                "imap_mag.plot.plot_ialirt_files.create_figure",
                return_value=(tmp_path / "out.png", mock_handler),
            ) as mock_create,
        ):
            plot_ialirt_files(
                science_files=[Path("science.csv")],
                hk_files=[],
                save_folder=tmp_path,
                combine_plots=True,
            )

        mock_create.assert_called_once()

    def test_calls_create_figure_per_date_when_not_combined(self, tmp_path):
        # 4 rows spanning 2 days (17th 00:00, 17th 12:00, 18th 00:00, 18th 12:00)
        idx = pd.date_range("2025-10-17", periods=4, freq="12h", tz="UTC")
        df = pd.DataFrame(
            {col: [float(i) for i in range(4)] for col in _IALIRT_COLUMNS},
            index=idx,
        )
        mock_handler = MagicMock()

        with (
            patch(
                "imap_mag.plot.plot_ialirt_files._load_csv_files",
                side_effect=[df, pd.DataFrame()],
            ),
            patch(
                "imap_mag.plot.plot_ialirt_files.create_figure",
                return_value=(tmp_path / "out.png", mock_handler),
            ) as mock_create,
        ):
            plot_ialirt_files(
                science_files=[Path("science.csv")],
                hk_files=[],
                save_folder=tmp_path,
                combine_plots=False,
            )

        assert mock_create.call_count == 2


class TestCreateFigureFunction:
    def test_saves_png_and_returns_path_and_handler(self, tmp_path):
        df = _make_ialirt_df()
        mock_db = MagicMock()
        mock_db.get_workflow_progress.return_value.get_progress_timestamp.return_value = None
        mock_db.get_workflow_progress.return_value.get_last_checked_date.return_value = None

        with patch(
            "imap_mag.plot.plot_ialirt_files.Database",
            return_value=mock_db,
        ):
            output_file, handler = create_figure(df, tmp_path)

        assert output_file.suffix == ".png"
        assert output_file.exists()
        assert handler is not None


class TestSetTimeFormatFunction:
    def test_applies_time_formatter_to_figure_axes(self):
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots()
        idx = pd.date_range("2025-10-17", periods=3, freq="1h")
        ax.plot(idx, [1, 2, 3])

        set_time_format(fig)
        plt.close(fig)
