from datetime import datetime
from pathlib import Path

from imap_mag.cli.calibrate import calibrate, gradiometry
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationMethod, Sensor
from tests.test_apply import copy_test_file
from tests.util.miscellaneous import (  # noqa: F401
    TEST_DATA,
    create_test_file,
    temp_datastore,
)


def test_empty_calibrator_makes_correct_matlab_call(
    monkeypatch,
    temp_datastore,  # noqa: F811
    preclean_work_and_output,
):
    copy_test_file(
        temp_datastore / "science/mag/l1c/2025/10",
        TEST_DATA / "imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        "imap_mag_l1c_norm-mago_20251017_v000.cdf",
    )

    def mock_call_matlab(command):
        assert (
            command
            == f'calibration.wrappers.run_empty_calibrator("2025-10-17T00:00:00", ".work/imap_mag_l1c_norm-mago_20251017_v001.cdf", ".work/imap_mag_noop-layer_20251017_v002.json", ".work/imap_mag_noop-layer-data_20251017_v002.csv", "{temp_datastore}", "")'
        )
        create_test_file(Path(".work") / "imap_mag_noop-layer_20251017_v002.json")
        create_test_file(Path(".work") / "imap_mag_noop-layer-data_20251017_v002.csv")

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

    assert (
        temp_datastore
        / "calibration/layers/2025/10/imap_mag_noop-layer_20251017_v002.json"
    ).exists()
    assert (
        temp_datastore
        / "calibration/layers/2025/10/imap_mag_noop-layer-data_20251017_v002.csv"
    ).exists()


def test_gradiometer_calibrator_makes_correct_matlab_call(
    monkeypatch,
    temp_datastore,  # noqa: F811
    preclean_work_and_output,
):
    def mock_call_matlab(command):
        assert (
            command
            == f'calibration.wrappers.run_gradiometry("2026-09-30T00:00:00", ".work/imap_mag_l1c_norm-mago_20260930_v001.cdf", ".work/imap_mag_l1c_norm-magi_20260930_v001.cdf", ".work/imap_mag_gradiometer-layer_20260930_v001.json", ".work/imap_mag_gradiometer-layer-data_20260930_v001.csv", "{temp_datastore}", "0.25", "10.0")'
        )
        create_test_file(
            Path(".work") / "imap_mag_gradiometer-layer_20260930_v001.json"
        )
        create_test_file(
            Path(".work") / "imap_mag_gradiometer-layer-data_20260930_v001.csv"
        )

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

    layer_metadata = (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-layer_20260930_v001.json"
    )
    assert layer_metadata.exists()

    layer_data = (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-layer_20260930_v001.json"
    )
    assert layer_data.exists()


def test_gradiometer_calibrator_finds_next_viable_version(
    monkeypatch,
    temp_datastore,  # noqa: F811
):
    def mock_call_matlab(command):
        assert (
            command
            == f'calibration.wrappers.run_gradiometry("2026-09-30T00:00:00", ".work/imap_mag_l1c_norm-mago_20260930_v001.cdf", ".work/imap_mag_l1c_norm-magi_20260930_v001.cdf", ".work/imap_mag_gradiometer-layer_20260930_v002.json", ".work/imap_mag_gradiometer-layer-data_20260930_v002.csv", "{temp_datastore}", "0.25", "10.0")'
        )
        create_test_file(
            Path(".work") / "imap_mag_gradiometer-layer_20260930_v002.json"
        )
        create_test_file(
            Path(".work") / "imap_mag_gradiometer-layer-data_20260930_v002.csv"
        )

    monkeypatch.setattr(
        "mag_toolkit.calibration.calibrators.GradiometerCalibration.call_matlab",
        mock_call_matlab,
    )

    existing_layer = (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-layer_20260930_v001.json"
    )
    existing_layer.parent.mkdir(parents=True, exist_ok=True)
    existing_layer.touch()

    gradiometry(
        date=datetime(2026, 9, 30),
        mode=ScienceMode.Normal,
        kappa=0.25,
        sc_interference_threshold=10.0,
    )

    layer_metadata = (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-layer_20260930_v002.json"
    )
    assert layer_metadata.exists()

    layer_data = (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-layer_20260930_v002.json"
    )
    assert layer_data.exists()
