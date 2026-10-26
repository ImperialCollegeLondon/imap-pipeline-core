"""Tests for the calibrate CLI, with the MATLAB scripted calibration mocked out."""

from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.cli.calibrate import calibrate
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationMethod, LayerDataFormat, Sensor
from mag_toolkit.calibration.CalibrationConfig import ScriptedL2CalibrationConfig
from mag_toolkit.calibration.calibrators.ScriptedL2Calibration import (
    OUTPUT_SUBFOLDER_NAME,
)
from tests.util.miscellaneous import (
    make_fake_matlab_calibration_repo,
    write_calibration_layer_pair,
)

MODULE_CALL_MATLAB = (
    "mag_toolkit.calibration.calibrators.ScriptedL2Calibration.call_matlab"
)
DATE = datetime(2026, 9, 30)
DESCRIPTOR = "manual-norm"


def _matlab_output_folder(
    base: Path, date: datetime = DATE, mode: str = "norm"
) -> Path:
    """Where the scripted calibration has MATLAB write its outputs, inside the
    calibrate command's dynamic work folder (base/calibrate_{date}_{mode})."""
    return base / f"calibrate_{date.strftime('%Y%m%d')}_{mode}" / OUTPUT_SUBFOLDER_NAME


def _run_scripted_calibration(tmp_path: Path) -> list[Path]:
    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=9,
        input_json_file="input.json",
        matlab_repo=str(make_fake_matlab_calibration_repo(tmp_path)),
    )
    return calibrate(
        start_date=DATE,
        method=CalibrationMethod.SCRIPTED_L2_CALIBRATION,
        mode=ScienceMode.Normal,
        configuration=config.model_dump_json(),
        metakernel=Path("metakernel.txt"),
        layer_data_format=LayerDataFormat.CSV,
    )


def _mock_matlab_writing_layer(monkeypatch, dynamic_work_folder, seed: int = 0) -> None:
    """MATLAB always emits v001.0001; versioning is the datastore's job."""

    def mock_call_matlab(command, **kwargs):
        write_calibration_layer_pair(
            _matlab_output_folder(dynamic_work_folder), DESCRIPTOR, DATE, 1, seed=seed
        )

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)


def test_calibrate_returns_list(
    monkeypatch, temp_datastore, dynamic_work_folder, tmp_path
):
    """calibrate() returns a list of Path objects (not a single Path or tuple)."""
    _mock_matlab_writing_layer(monkeypatch, dynamic_work_folder)

    result = _run_scripted_calibration(tmp_path)

    assert isinstance(result, list)
    assert len(result) >= 1
    assert all(isinstance(p, Path) for p in result)


def test_noop_method_is_not_runnable(temp_datastore, dynamic_work_folder):
    """The do-nothing 'noop' calibrator has been removed as a selectable method."""
    with pytest.raises(ValueError, match="not runnable"):
        calibrate(
            start_date=datetime(2025, 10, 17),
            sensor=Sensor.MAGO,
            mode=ScienceMode.Normal,
            method=CalibrationMethod.NOOP,
        )


def test_calibration_finds_next_viable_version(
    monkeypatch, temp_datastore, dynamic_work_folder, tmp_path
):
    """Calibration always produces v001 output; datastore bumps to v002 when v001 already exists."""
    _mock_matlab_writing_layer(monkeypatch, dynamic_work_folder, seed=0)

    # Pre-populate datastore with a different v001 (different seed -> different content)
    layers_dir = temp_datastore / "calibration/layers/2026/09"
    layers_dir.mkdir(parents=True, exist_ok=True)
    write_calibration_layer_pair(layers_dir, DESCRIPTOR, DATE, 1, seed=1)

    _run_scripted_calibration(tmp_path)

    assert (layers_dir / "imap_mag_manual-norm-layer_20260930_v001.0002.json").exists()
    assert (
        layers_dir / "imap_mag_manual-norm-layer-data_20260930_v001.0002.csv"
    ).exists()


def test_calibration_layer_versioned_together_when_only_json_exists(
    monkeypatch, temp_datastore, dynamic_work_folder, tmp_path
):
    """When only a JSON layer exists at v001 (no CSV), both JSON and CSV
    output files must be versioned as v002 - not JSON=v002, CSV=v001."""
    _mock_matlab_writing_layer(monkeypatch, dynamic_work_folder, seed=0)

    layers_dir = temp_datastore / "calibration/layers/2026/09"
    layers_dir.mkdir(parents=True, exist_ok=True)
    _, existing_csv = write_calibration_layer_pair(
        layers_dir, DESCRIPTOR, DATE, 1, seed=1
    )
    existing_csv.unlink()  # Leave only JSON at v001

    _run_scripted_calibration(tmp_path)

    assert (
        layers_dir / "imap_mag_manual-norm-layer_20260930_v001.0002.json"
    ).exists(), "JSON layer must be v001.0002"
    assert (
        layers_dir / "imap_mag_manual-norm-layer-data_20260930_v001.0002.csv"
    ).exists(), "CSV data file must also be v001.0002, not v001.0001"


def test_calibration_layer_versioned_together_when_only_csv_exists(
    monkeypatch, temp_datastore, dynamic_work_folder, tmp_path
):
    """When only a CSV layer-data exists at v001 (no JSON), both JSON and CSV
    output files must be versioned as v002 - not JSON=v001, CSV=v002.
    The sibling-check in DatastoreFileManager ensures the JSON version is
    also bumped when the companion CSV slot is already occupied."""
    _mock_matlab_writing_layer(monkeypatch, dynamic_work_folder, seed=0)

    # Its content differs from the new CSV so the sibling check must block v001.
    layers_dir = temp_datastore / "calibration/layers/2026/09"
    layers_dir.mkdir(parents=True, exist_ok=True)
    existing_json, _ = write_calibration_layer_pair(
        layers_dir, DESCRIPTOR, DATE, 1, seed=1
    )
    existing_json.unlink()  # Leave only CSV at v001

    _run_scripted_calibration(tmp_path)

    assert (
        layers_dir / "imap_mag_manual-norm-layer_20260930_v001.0002.json"
    ).exists(), "JSON layer must be v001.0002"
    assert (
        layers_dir / "imap_mag_manual-norm-layer-data_20260930_v001.0002.csv"
    ).exists(), "CSV data file must also be v001.0002, not v001.0001"
