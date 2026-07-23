from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.cli.calibrate import calibrate, gradiometry
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationMethod, Sensor
from tests.util.miscellaneous import (
    write_calibration_layer_pair,
)


def _cal_work_folder(base: Path, date: datetime, mode: str = "norm") -> Path:
    """The calibrate command's dynamic work folder (base/calibrate_{date}_{mode})."""
    return base / f"calibrate_{date.strftime('%Y%m%d')}_{mode}"


def test_noop_method_is_not_runnable(temp_datastore, dynamic_work_folder):
    """The do-nothing 'noop' calibrator has been removed as a selectable method."""
    with pytest.raises(ValueError, match="not runnable"):
        calibrate(
            start_date=datetime(2025, 10, 17),
            sensor=Sensor.MAGO,
            mode=ScienceMode.Normal,
            method=CalibrationMethod.NOOP,
        )


def test_gradiometer_calibrator_makes_correct_matlab_call(
    monkeypatch,
    temp_datastore,
    dynamic_work_folder,
):
    def mock_call_matlab(command):
        assert command.startswith("calibration.wrappers.run_gradiometry")
        write_calibration_layer_pair(
            _cal_work_folder(dynamic_work_folder, datetime(2026, 9, 30)),
            "gradiometer-norm",
            datetime(2026, 9, 30),
            1,
        )

    monkeypatch.setattr(
        "mag_toolkit.calibration.calibrators.GradiometerCalibration.call_matlab",
        mock_call_matlab,
    )

    gradiometry(
        start_date=datetime(2026, 9, 30),
        mode=ScienceMode.Normal,
        kappa=0.25,
        sc_interference_threshold=10.0,
    )

    layer_metadata = (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-norm-layer_20260930_v001.0001.json"
    )
    assert layer_metadata.exists()

    layer_data = (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-norm-layer-data_20260930_v001.0001.csv"
    )
    assert layer_data.exists()


def test_gradiometer_calibrator_finds_next_viable_version(
    monkeypatch,
    temp_datastore,
    dynamic_work_folder,
):
    """Calibration always produces v001 output; datastore bumps to v002 when v001 already exists."""

    work = _cal_work_folder(dynamic_work_folder, datetime(2026, 9, 30))

    def mock_call_matlab(command):
        assert (
            command
            == f'calibration.wrappers.run_gradiometry("2026-09-30T00:00:00", "{work}/imap_mag_l1c_norm-mago_20260930_v001.cdf", "{work}/imap_mag_l1c_norm-magi_20260930_v001.cdf", "{work}/imap_mag_gradiometer-norm-layer_20260930_v001.0001.json", "{work}/imap_mag_gradiometer-norm-layer-data_20260930_v001.0001.csv", "{temp_datastore}", "0.25", "10.0")'
        )
        write_calibration_layer_pair(
            work, "gradiometer-norm", datetime(2026, 9, 30), 1, seed=0
        )

    monkeypatch.setattr(
        "mag_toolkit.calibration.calibrators.GradiometerCalibration.call_matlab",
        mock_call_matlab,
    )

    # Pre-populate datastore with a different v001 (different seed → different content)
    layers_dir = temp_datastore / "calibration/layers/2026/09"
    layers_dir.mkdir(parents=True, exist_ok=True)
    write_calibration_layer_pair(
        layers_dir, "gradiometer-norm", datetime(2026, 9, 30), 1, seed=1
    )

    gradiometry(
        start_date=datetime(2026, 9, 30),
        mode=ScienceMode.Normal,
        kappa=0.25,
        sc_interference_threshold=10.0,
    )

    # Datastore must have versioned up to v001.0002
    assert (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-norm-layer_20260930_v001.0002.json"
    ).exists()
    assert (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-norm-layer-data_20260930_v001.0002.csv"
    ).exists()


def test_calibration_layer_versioned_together_when_only_json_exists(
    monkeypatch,
    temp_datastore,
    dynamic_work_folder,
):
    """When only a JSON layer exists at v001 (no CSV), both JSON and CSV
    output files must be versioned as v002 — not JSON=v002, CSV=v001.
    Calibration always produces v001; the datastore bumps both to v002."""

    def mock_call_matlab(command):
        # Calibration always emits v001 — versioning is the datastore's job
        write_calibration_layer_pair(
            _cal_work_folder(dynamic_work_folder, datetime(2026, 9, 30)),
            "gradiometer-norm",
            datetime(2026, 9, 30),
            1,
            seed=0,
        )

    monkeypatch.setattr(
        "mag_toolkit.calibration.calibrators.GradiometerCalibration.call_matlab",
        mock_call_matlab,
    )

    # Pre-populate the datastore with only a JSON at v001 (no matching CSV)
    layers_dir = temp_datastore / "calibration/layers/2026/09"
    layers_dir.mkdir(parents=True, exist_ok=True)
    _, existing_csv = write_calibration_layer_pair(
        layers_dir, "gradiometer-norm", datetime(2026, 9, 30), 1, seed=1
    )
    existing_csv.unlink()  # Leave only JSON at v001

    gradiometry(
        start_date=datetime(2026, 9, 30),
        mode=ScienceMode.Normal,
        kappa=0.25,
        sc_interference_threshold=10.0,
    )

    # Both files must share version v001.0002
    assert (
        layers_dir / "imap_mag_gradiometer-norm-layer_20260930_v001.0002.json"
    ).exists(), "JSON layer must be v001.0002"
    assert (
        layers_dir / "imap_mag_gradiometer-norm-layer-data_20260930_v001.0002.csv"
    ).exists(), "CSV data file must also be v001.0002, not v001.0001"


def test_calibration_layer_versioned_together_when_only_csv_exists(
    monkeypatch,
    temp_datastore,
    dynamic_work_folder,
):
    """When only a CSV layer-data exists at v001 (no JSON), both JSON and CSV
    output files must be versioned as v002 — not JSON=v001, CSV=v002.
    The sibling-check in DatastoreFileManager ensures the JSON version is
    also bumped when the companion CSV slot is already occupied."""

    def mock_call_matlab(command):
        # Calibration always emits v001 — versioning is the datastore's job
        write_calibration_layer_pair(
            _cal_work_folder(dynamic_work_folder, datetime(2026, 9, 30)),
            "gradiometer-norm",
            datetime(2026, 9, 30),
            1,
            seed=0,
        )

    monkeypatch.setattr(
        "mag_toolkit.calibration.calibrators.GradiometerCalibration.call_matlab",
        mock_call_matlab,
    )

    # Pre-populate the datastore with only a CSV at v001 (no matching JSON).
    # Its content differs from the new CSV so the sibling check must block v001.
    layers_dir = temp_datastore / "calibration/layers/2026/09"
    layers_dir.mkdir(parents=True, exist_ok=True)
    existing_json, _ = write_calibration_layer_pair(
        layers_dir, "gradiometer-norm", datetime(2026, 9, 30), 1, seed=1
    )
    existing_json.unlink()  # Leave only CSV at v001

    gradiometry(
        start_date=datetime(2026, 9, 30),
        mode=ScienceMode.Normal,
        kappa=0.25,
        sc_interference_threshold=10.0,
    )

    # Both files must share version v001.0002
    assert (
        layers_dir / "imap_mag_gradiometer-norm-layer_20260930_v001.0002.json"
    ).exists(), "JSON layer must be v001.0002"
    assert (
        layers_dir / "imap_mag_gradiometer-norm-layer-data_20260930_v001.0002.csv"
    ).exists(), "CSV data file must also be v001.0002, not v001.0001"
