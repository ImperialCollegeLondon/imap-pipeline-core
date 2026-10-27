import logging
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from imap_mag.appLogging import AppLogging
from imap_mag.util import DatetimeProvider, Environment
from mag_toolkit.calibration.CalibrationDefinitions import LayerDataFormat

NOW = datetime(2025, 10, 14, 12, 37, 9)
TODAY = NOW.replace(hour=0, minute=0, second=0, microsecond=0)  # 2025-10-14
TOMORROW = TODAY + timedelta(days=1)  # 2025-10-15
YESTERDAY = TODAY - timedelta(days=1)  # 2025-10-13
START_OF_HOUR = NOW.replace(minute=0, second=0, microsecond=0)  # 2025-10-14 12:00:00
END_OF_HOUR = NOW.replace(minute=59, second=59, microsecond=999999)
END_OF_TODAY = TODAY.replace(hour=23, minute=59, second=59, microsecond=999999)
BEGINNING_OF_IMAP = datetime(2025, 9, 24, 0, 0, 0)  # actual IMAP launch date


DATASTORE = Path("tests/datastore")
TEST_DATA = Path("tests/test_data")
TEST_TRUTH = Path("tests/test_truth")

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=False)
def enableLogging():
    AppLogging.set_up_logging(
        console_log_output="stdout",
        console_log_level="debug",
        console_log_color=True,
        logfile_file="debug",
        logfile_log_level="debug",
        logfile_log_color=False,
        log_line_template="%(color_on)s[%(asctime)s] [%(levelname)-5s] %(message)s%(color_off)s",
        console_log_line_template="%(color_on)s[%(levelname)-5s] %(name)s %(message)s%(color_off)s",
    )
    yield
    AppLogging.reset_setup_flag()  # Reset logging setup after test


@pytest.fixture(autouse=False, scope="function")
def temp_datastore():
    temp_datastore = Path(tempfile.mkdtemp())
    shutil.copytree(DATASTORE, temp_datastore, dirs_exist_ok=True)

    with Environment(MAG_DATA_STORE=str(temp_datastore)):
        yield temp_datastore

    shutil.rmtree(temp_datastore, ignore_errors=True)


@pytest.fixture(autouse=False)
def fixed_datetime_provider() -> DatetimeProvider:
    """Return a DatetimeProvider fixed to the test constants time."""
    return DatetimeProvider(fixed_now=NOW)


def create_test_file(file_path: Path, content: str | None = None) -> Path:
    """Create a file with the given content."""

    file_path.unlink(missing_ok=True)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    file_path.touch()

    if content is not None:
        file_path.write_text(content)

    return file_path


def copy_test_file(src_file: Path, dest_folder: Path, new_filename: str | None = None):
    if not new_filename:
        new_filename = src_file.name

    shutil.copy(
        src_file,
        dest_folder / new_filename,
    )


def write_calibration_layer_pair(
    folder: Path,
    descriptor: str,
    date: datetime,
    version: int,
    seed: int = 0,
    layer_data_format: LayerDataFormat | None = None,
) -> tuple[Path, Path]:
    """Create a real CalibrationLayer JSON + data file pair in *folder*.

    Args:
        folder: Folder to write the pair into.
        descriptor: Layer descriptor, e.g. "manual-norm".
        date: Content date of the layer.
        version: Minor version of the layer files.
        seed: Varies the epoch and offsets so different seeds give different content.
        layer_data_format: Format of the data file, CSV when not given.

    Returns:
        (json_path, data_path)
    """
    from imap_mag.io.file import CalibrationLayerPathHandler
    from mag_toolkit.calibration.CalibrationDefinitions import (
        CalibrationMetadata,
        CalibrationMethod,
        LayerDataFormat,
        Mission,
        Sensor,
        Validity,
        ValueType,
    )
    from mag_toolkit.calibration.CalibrationLayer import CalibrationLayer

    epoch = np.datetime64(date) + np.timedelta64(seed, "s")
    contents = pd.DataFrame(
        {
            "time": np.array([epoch], dtype="datetime64[ns]"),
            "offset_x": [float(seed)],
            "offset_y": [float(seed)],
            "offset_z": [float(seed)],
            "timedelta": [0.0],
            "quality_flag": [0],
            "quality_bitmask": [0],
        }
    )
    layer = CalibrationLayer(
        id="",
        mission=Mission.IMAP,
        validity=Validity(start=epoch, end=epoch),
        sensor=Sensor.MAGO,
        version=version,
        version_major=1,
        metadata=CalibrationMetadata(
            dependencies=[],
            science=[],
            creation_timestamp=np.datetime64("now"),
            content_date=np.datetime64(date),
        ),
        value_type=ValueType.VECTOR,
        method=CalibrationMethod.NOOP,
    )
    handler = CalibrationLayerPathHandler(
        descriptor=descriptor,
        content_date=date,
        version=version,
        version_major=1,
    )
    data_handler = handler.create_new_datafile_handler(
        layer_data_format or LayerDataFormat.CSV
    )
    layer.metadata.data_filename = Path(data_handler.get_filename())
    layer._contents = contents

    json_path = folder / handler.get_filename()
    layer.write_to_file(json_path)

    csv_path = folder / data_handler.get_filename()
    return json_path, csv_path


def open_cdf(cdf_path: Path, readonly: bool = True):
    from spacepy import (
        pycdf,  # has race condition when imported in multiple threads, so import locally within function. See https://github.com/spacepy/spacepy/issues/835
    )

    return pycdf.CDF(str(cdf_path), readonly=readonly)


def make_fake_matlab_calibration_repo(folder: Path, version: str | None = None) -> Path:
    """Create a stand-in for the IMAP_MAG_Calibration MATLAB repository.

    Args:
        folder: Folder to create the repository in (as ``folder / "repo"``).
        version: Version for ``+calibration/get_version.m`` to report, e.g.
            "v3.5.0". No version file is written when not given.

    Returns:
        Path to the repository root.
    """
    repo = folder / "repo"
    script = repo / "+calibration" / "+scripts" / "calibrate_l2_offsets.m"
    script.parent.mkdir(parents=True)
    script.write_text("function calibrate_l2_offsets(); end")
    if version is not None:
        (repo / "+calibration" / "get_version.m").write_text(
            f'function v=get_version()\nv="{version}";\nend\n'
        )
    return repo
