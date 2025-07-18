from datetime import datetime
from pathlib import Path

from imap_mag.cli.calibrate import calibrate, gradiometry
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationMethod, Sensor
from tests.test_calibration import prepare_test_file

from .util.miscellaneous import (  # noqa: F401
    create_test_file,
    tidyDataFolders,
)


def test_empty_calibrator_makes_correct_matlab_call(monkeypatch, tmp_path):
    prepare_test_file(
        "imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        "science/mag/l1c",
        2025,
        10,
        rename="imap_mag_l1c_norm-mago_20251017_v000.cdf",
    )

    def mock_call_matlab(command):
        assert (
            command
            == 'calibration.wrappers.run_empty_calibrator("2025-10-17T00:00:00", ".work/imap_mag_l1c_norm-mago_20251017_v000.cdf", ".work/imap_mag_noop-layer_20251017_v001.json", "output", "")'
        )
        temp_file = Path(".work") / "imap_mag_noop-layer_20251017_v001.json"
        create_test_file(temp_file)

    monkeypatch.setattr(
        "mag_toolkit.calibration.calibrators.EmptyCalibration.call_matlab",
        mock_call_matlab,
    )

    calibrate(
        date=datetime(2025, 10, 17),
        sensor=Sensor.MAGO,
        mode=ScienceMode.Normal,
        method=CalibrationMethod.NOOP,
    )

    assert Path(
        "output/calibration/layers/2025/10/imap_mag_noop-layer_20251017_v001.json"
    ).exists()


def test_gradiometer_calibrator_makes_correct_matlab_call(monkeypatch, tmp_path):
    prepare_test_file(
        "imap_mag_l1c_norm-mago_20260930_v001.cdf",
        "science/mag/l1c",
        2026,
        9,
    )

    prepare_test_file(
        "imap_mag_l1c_norm-magi_20260930_v001.cdf",
        "science/mag/l1c",
        2026,
        9,
    )

    def mock_call_matlab(command):
        assert (
            command
            == 'calibration.wrappers.run_gradiometry("2026-09-30T00:00:00", ".work/imap_mag_l1c_norm-mago_20260930_v001.cdf", ".work/imap_mag_l1c_norm-magi_20260930_v001.cdf", ".work/imap_mag_gradiometer-layer_20260930_v001.json", "output", "0.25", "10.0")'
        )
        temp_file = Path(".work") / "imap_mag_gradiometer-layer_20260930_v001.json"
        create_test_file(temp_file)

    monkeypatch.setattr(
        "mag_toolkit.calibration.calibrators.GradiometerCalibration.call_matlab",
        mock_call_matlab,
    )

    gradiometry(
        date=datetime(2026, 9, 30),
        mode=ScienceMode.Normal,
        kappa=0.25,
        sc_interference_threshold=10.0,
    )
    output_file = "output/calibration/layers/2026/09/imap_mag_gradiometer-layer_20260930_v001.json"
    assert Path(output_file).exists()
