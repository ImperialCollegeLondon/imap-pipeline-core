"""Tests for `FetchIALiRT` class."""

import shutil
import tempfile
from datetime import datetime
from pathlib import Path

from diffimg import diff

from imap_mag.cli.plot.plot_ialirt import plot_ialirt
from tests.util.miscellaneous import TEST_DATA, TEST_TRUTH, temp_datastore  # noqa: F401

IALIRT_PACKET_DEFINITION = (
    Path(__file__).parent.parent / "src" / "imap_mag" / "packet_def"
)


def test_plot_ialirt(
    temp_datastore: Path,  # noqa: F811
    capture_cli_logs,
) -> None:
    # Set up.
    test_data = TEST_DATA / "ialirt_plot_data.csv"

    (temp_datastore / "ialirt" / "2025" / "10").mkdir(parents=True, exist_ok=True)
    shutil.copy(
        test_data,
        temp_datastore / "ialirt" / "2025" / "10" / "imap_ialirt_20251017.csv",
    )

    expected_figure = TEST_TRUTH / "ialirt_quicklook.png"

    # Execute.
    generated_plots = plot_ialirt(
        start_date=datetime(2025, 10, 17, 0, 0, 0),
        end_date=datetime(2025, 10, 17, 23, 59, 59),
    )

    # Verify.
    assert len(generated_plots) == 1

    ((plot_file, path_handler),) = generated_plots.items()
    assert plot_file.exists()

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
