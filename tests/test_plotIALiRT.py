"""Tests for `FetchIALiRT` class."""

import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from diffimg import diff

from imap_mag.cli.plot.plot_ialirt import plot_ialirt
from imap_mag.config import SaveMode
from imap_mag.util import CONSTANTS, DatetimeProvider
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    END_OF_TODAY,
    NOW,
    TEST_DATA,
    TEST_TRUTH,
    TODAY,
    mock_datetime_provider,  # noqa: F401
    temp_datastore,  # noqa: F401
)

IALIRT_PACKET_DEFINITION = (
    Path(__file__).parent.parent / "src" / "imap_mag" / "packet_def"
)


@pytest.fixture(scope="function", autouse=False)
def mock_datetime_provider_today_20251021(monkeypatch):
    monkeypatch.setattr(DatetimeProvider, "today", lambda: datetime(2025, 10, 21))


def _setup_ialirt_datastore(
    temp_datastore: Path,  # noqa: F811
    date_str: str,
    date_fmt: str,
):
    """Set up both science and HK files in the datastore for a given date."""

    science_data = TEST_DATA / "ialirt_science_plot_data.csv"
    hk_data = TEST_DATA / "ialirt_hk_plot_data.csv"

    (temp_datastore / "ialirt" / date_fmt).mkdir(parents=True, exist_ok=True)
    shutil.copy(
        science_data,
        temp_datastore / "ialirt" / date_fmt / f"imap_ialirt_{date_str}.csv",
    )

    (temp_datastore / "ialirt_hk" / date_fmt).mkdir(parents=True, exist_ok=True)
    shutil.copy(
        hk_data,
        temp_datastore / "ialirt_hk" / date_fmt / f"imap_ialirt_hk_{date_str}.csv",
    )


def test_plot_ialirt(
    temp_datastore: Path,  # noqa: F811
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
) -> None:
    # Set up.
    _setup_ialirt_datastore(
        temp_datastore, TODAY.strftime("%Y%m%d"), TODAY.strftime("%Y/%m")
    )

    ialirt_progress = test_database.get_workflow_progress(
        CONSTANTS.DATABASE.IALIRT_PROGRESS_ID
    )
    ialirt_progress.update_progress_timestamp(NOW)
    test_database.save(ialirt_progress)

    validation_progress = test_database.get_workflow_progress(
        CONSTANTS.DATABASE.IALIRT_VALIDATION_ID
    )
    validation_progress.update_last_checked_timestamp(NOW - timedelta(hours=1))
    test_database.save(validation_progress)

    expected_figure = TEST_TRUTH / "ialirt_quicklook.png"

    # Execute.
    generated_plots = plot_ialirt(start_date=TODAY, end_date=END_OF_TODAY)

    # Verify.
    assert len(generated_plots) == 1

    ((plot_file, path_handler),) = generated_plots.items()
    assert plot_file.exists()
    assert not (temp_datastore / "quicklook" / "ialirt" / "latest.png").exists()

    assert path_handler.content_date == datetime(2025, 10, 21, 8, 36, 23)

    diff_file = Path(tempfile.gettempdir()) / "result_diff.png"
    image_diff = diff(
        expected_figure,
        plot_file,
        delete_diff_file=True,
        diff_img_file=str(diff_file),
        ignore_alpha=False,
    )

    print(f"Image diff: {image_diff} . diff file is at {diff_file!s}")

    assert image_diff is not None
    assert image_diff < 0.0001


def test_plot_ialirt_todays_data_copies_to_latest_figure(
    temp_datastore: Path,  # noqa: F811
    test_database,  # noqa: F811
    mock_datetime_provider_today_20251021,
) -> None:
    # Set up.
    _setup_ialirt_datastore(temp_datastore, "20251021", "2025/10")

    # Execute.
    generated_plots = plot_ialirt(
        start_date=datetime(2025, 10, 21, 0, 0, 0),
        end_date=datetime(2025, 10, 21, 23, 59, 59),
    )

    # Verify.
    assert len(generated_plots) == 1
    assert (temp_datastore / "quicklook" / "ialirt" / "latest.png").exists()


def test_plot_ialirt_todays_data_added_to_database(
    temp_datastore: Path,  # noqa: F811
    test_database,  # noqa: F811
    mock_datetime_provider_today_20251021,
) -> None:
    # Set up.
    _setup_ialirt_datastore(temp_datastore, "20251021", "2025/10")

    assert len(test_database.get_files()) == 0

    # Execute.
    generated_plots = plot_ialirt(
        start_date=datetime(2025, 10, 21, 0, 0, 0),
        end_date=datetime(2025, 10, 21, 23, 59, 59),
        save_mode=SaveMode.LocalAndDatabase,
    )

    # Verify.
    assert len(generated_plots) == 1
    assert (temp_datastore / "quicklook" / "ialirt" / "latest.png").exists()

    files_in_db = test_database.get_files()
    assert len(files_in_db) == 2

    assert any(
        file.path == "quicklook/ialirt/2025/10/imap_quicklook_ialirt_20251021.png"
        for file in files_in_db
    )
    assert any(file.path == "quicklook/ialirt/latest.png" for file in files_in_db)
