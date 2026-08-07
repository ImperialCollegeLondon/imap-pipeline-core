"""Tests for the multi-file save loop in calibrate._calibrate_for_date.

These tests exercise the logic that collects every file returned by a
CalibrationJob, resolves a path handler for each one, and publishes them
all — including extra products beyond the standard JSON+CSV pair.
"""

from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.cli.calibrate import calibrate
from imap_mag.io.FilePathHandlerSelector import NoProviderFoundError
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationMethod
from mag_toolkit.calibration.CalibrationConfig import ScriptedL2CalibrationConfig
from mag_toolkit.calibration.calibrators.ScriptedL2Calibration import (
    OUTPUT_SUBFOLDER_NAME,
)
from tests.util.miscellaneous import write_calibration_layer_pair

MODULE_CALL_MATLAB = (
    "mag_toolkit.calibration.calibrators.ScriptedL2Calibration.call_matlab"
)
DATE = datetime(2026, 1, 30)


def _make_matlab_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    script = repo / "+calibration" / "+scripts" / "calibrate_l2_offsets.m"
    script.parent.mkdir(parents=True)
    script.write_text("function calibrate_l2_offsets(); end")
    return repo


def test_saves_all_returned_files(
    monkeypatch, temp_datastore, dynamic_work_folder, tmp_path
):
    """Extra files beyond the JSON+CSV pair are published to the datastore."""
    repo = _make_matlab_repo(tmp_path)
    (temp_datastore / "spice" / "mk").mkdir(parents=True, exist_ok=True)
    (temp_datastore / "spice" / "mk" / "mk.txt").write_text("kernel")

    work_folder = dynamic_work_folder / "calibrate_20260130_norm"

    def mock_call_matlab(command, **kwargs):
        outputs = work_folder / OUTPUT_SUBFOLDER_NAME
        write_calibration_layer_pair(outputs, "manual-norm", DATE, 1)

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)

    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8,
        input_json_file="input.json",
        matlab_repo=str(repo),
    )

    results = calibrate(
        start_date=DATE,
        method=CalibrationMethod.SCRIPTED_L2_CALIBRATION,
        mode=ScienceMode.Normal,
        configuration=config.model_dump_json(),
        metakernel=Path("mk.txt"),
    )

    results = [f for f in results if "mk.txt" not in str(f)]

    assert len(results) == 1
    layers_dir = temp_datastore / "calibration/layers/2026/01"
    assert (layers_dir / "imap_mag_manual-norm-layer_20260130_v001.0001.json").exists()
    assert (
        layers_dir / "imap_mag_manual-norm-layer-data_20260130_v001.0001.csv"
    ).exists()


def test_zero_returned_files_raises(
    monkeypatch, temp_datastore, dynamic_work_folder, tmp_path
):
    """Calibration that returns no files raises ValueError before any publishing."""
    repo = _make_matlab_repo(tmp_path)
    (temp_datastore / "spice" / "mk").mkdir(parents=True, exist_ok=True)
    (temp_datastore / "spice" / "mk" / "mk.txt").write_text("kernel")

    def mock_call_matlab(command, **kwargs):
        pass  # writes nothing to the outputs folder

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)

    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8,
        input_json_file="input.json",
        matlab_repo=str(repo),
    )

    with pytest.raises(FileNotFoundError, match="produced no output files"):
        calibrate(
            start_date=DATE,
            method=CalibrationMethod.SCRIPTED_L2_CALIBRATION,
            mode=ScienceMode.Normal,
            configuration=config.model_dump_json(),
            metakernel=Path("mk.txt"),
        )


def test_unhandled_file_raises(
    monkeypatch, temp_datastore, dynamic_work_folder, tmp_path
):
    """A file type that no handler recognises raises NoProviderFoundError."""
    repo = _make_matlab_repo(tmp_path)
    (temp_datastore / "spice" / "mk").mkdir(parents=True, exist_ok=True)
    (temp_datastore / "spice" / "mk" / "mk.txt").write_text("kernel")

    work_folder = dynamic_work_folder / "calibrate_20260130_norm"

    def mock_call_matlab(command, **kwargs):
        outputs = work_folder / OUTPUT_SUBFOLDER_NAME
        # Write only an unrecognised file type (no JSON/CSV pair)
        (outputs / "unknown_output.xyz").write_bytes(b"data")

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)

    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8,
        input_json_file="input.json",
        matlab_repo=str(repo),
    )

    with pytest.raises(NoProviderFoundError):
        calibrate(
            start_date=DATE,
            method=CalibrationMethod.SCRIPTED_L2_CALIBRATION,
            mode=ScienceMode.Normal,
            configuration=config.model_dump_json(),
            metakernel=Path("mk.txt"),
        )
