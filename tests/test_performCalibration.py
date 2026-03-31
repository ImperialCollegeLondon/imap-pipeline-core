import os
import threading
from datetime import datetime
from shutil import which

import pytest

from imap_mag.config import SaveMode
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationMethod, Sensor
from prefect_server.performCalibration import (
    apply_flow,
    calibrate_and_apply_flow,
    calibrate_flow,
)
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401

with threading.Lock():
    from spacepy import pycdf


def get_test_matlab_command():
    if os.getenv("MLM_LICENSE_TOKEN") and (which("matlab-batch") is not None):
        return "matlab-batch"
    else:
        return "matlab"


def test_apply_flow_resolves_layer_patterns_and_discovers_science_file(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
    prefect_test_fixture,  # noqa: F811
):
    """Test that apply_flow resolves layer patterns and discovers science files by mode."""
    apply_flow(
        layers=["*noop*"],
        start_date=datetime(2026, 1, 16),
        mode=ScienceMode.Normal,
        save_mode=SaveMode.LocalOnly,
    )

    date = datetime(2026, 1, 16)
    output_l2_file = (
        temp_datastore
        / f"science/mag/l2-pre/{date.year}/{date.month:02d}/imap_mag_l2-pre_norm-srf_{date.year}{date.month:02d}{date.day:02d}_v001.cdf"
    )
    assert output_l2_file.exists()

    output_offsets_file = (
        temp_datastore
        / f"science-ancillary/l2-offsets/{date.year}/{date.month:02d}/imap_mag_l2-norm-offsets_{date.year}{date.month:02d}{date.day:02d}_{date.year}{date.month:02d}{date.day:02d}_v001.cdf"
    )
    assert output_offsets_file.exists()

    with pycdf.CDF(str(output_l2_file)) as cdf:
        assert "b_srf" in cdf
        assert "epoch" in cdf
        assert "magnitude" in cdf


@pytest.mark.skipif(
    not (os.getenv("MLM_LICENSE_FILE") or os.getenv("MLM_LICENSE_TOKEN"))
    or which(get_test_matlab_command()) is None,
    reason="MATLAB License not set or MATLAB is not available; skipping MATLAB tests",
)
def test_calibrate_flow_creates_calibration_layer(
    temp_datastore,
    dynamic_work_folder,
    prefect_test_fixture,  # noqa: F811
):
    """Test that calibrate_flow creates a calibration layer for a date range."""
    from mag_toolkit.calibration.MatlabWrapper import setup_matlab_path

    setup_matlab_path("src/matlab", get_test_matlab_command())

    results = calibrate_flow(
        start_date=datetime(2026, 1, 16),
        mode=ScienceMode.Normal,
        method=CalibrationMethod.NOOP,
        configuration=None,
        sensor=Sensor.MAGO,
        save_mode=SaveMode.LocalOnly,
    )

    assert len(results) == 1
    assert results[0].exists()
    assert "noop-norm-layer" in results[0].name


@pytest.mark.skipif(
    not (os.getenv("MLM_LICENSE_FILE") or os.getenv("MLM_LICENSE_TOKEN"))
    or which(get_test_matlab_command()) is None,
    reason="MATLAB License not set or MATLAB is not available; skipping MATLAB tests",
)
def test_calibrate_and_apply_flow_creates_output(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
    prefect_test_fixture,  # noqa: F811
):
    """Test that calibrate_and_apply_flow calibrates and applies in one flow."""
    from mag_toolkit.calibration.MatlabWrapper import setup_matlab_path

    setup_matlab_path("src/matlab", get_test_matlab_command())

    calibrate_and_apply_flow(
        start_date=datetime(2026, 1, 16),
        method=CalibrationMethod.NOOP,
        configuration=None,
        mode=ScienceMode.Normal,
        sensor=Sensor.MAGO,
        save_mode=SaveMode.LocalOnly,
    )

    date = datetime(2026, 1, 16)
    output_l2_file = (
        temp_datastore
        / f"science/mag/l2-pre/{date.year}/{date.month:02d}/imap_mag_l2-pre_norm-srf_{date.year}{date.month:02d}{date.day:02d}_v001.cdf"
    )
    assert output_l2_file.exists()
