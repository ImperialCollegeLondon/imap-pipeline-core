"""Unit tests for the scripted L2 calibration job and its CLI wiring.

These tests mock out the actual MATLAB call so they run without MATLAB.
"""

import json
from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.cli.calibrate import calibrate
from imap_mag.config.CalibrationConfig import (
    CalibrationConfig,
    ScriptedL2CalibrationConfig,
)
from imap_mag.io.file import CalibrationLayerPathHandler
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import (
    CalibrationJobParameters,
    CalibrationMethod,
    Sensor,
)
from mag_toolkit.calibration.calibrators.ScriptedL2Calibration import (
    USER_CONFIG_FILENAME,
    ScriptedL2CalibrationJob,
)
from tests.util.miscellaneous import write_calibration_layer_pair

MODULE_CALL_MATLAB = (
    "mag_toolkit.calibration.calibrators.ScriptedL2Calibration.call_matlab"
)
DATE = datetime(2026, 1, 30)


def _make_job(
    tmp_path: Path,
    metakernel: str | None = "metakernel.txt",
    create_metakernel: bool = True,
) -> ScriptedL2CalibrationJob:
    """Build a ScriptedL2CalibrationJob wired to temp folders.

    The work folder, repo path and datastore are accessible via the returned
    job's ``work_folder``, ``matlab_repo_path`` and ``data_store`` attributes.
    """
    work_folder = tmp_path / "work"
    work_folder.mkdir()
    repo = tmp_path / "repo"
    repo.mkdir()
    datastore = tmp_path / "datastore"
    (datastore / "spice" / "mk").mkdir(parents=True)
    if create_metakernel and metakernel:
        (datastore / "spice" / "mk" / metakernel).write_text("KERNELS_TO_LOAD = ()")

    params = CalibrationJobParameters(
        date=DATE, mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    job = ScriptedL2CalibrationJob(
        params, work_folder, matlab_repo_path=repo, metakernel=metakernel
    )
    job.setup_datastore(datastore)
    return job


def _handler(version: int) -> CalibrationLayerPathHandler:
    return CalibrationLayerPathHandler(
        descriptor="manual-norm", content_date=DATE, version=version
    )


def test_requires_matlab_repo_path(tmp_path):
    params = CalibrationJobParameters(
        date=DATE, mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    with pytest.raises(ValueError, match="repository path is required"):
        ScriptedL2CalibrationJob(params, tmp_path, matlab_repo_path=None)


def test_no_science_files_are_fetched(tmp_path):
    job = _make_job(tmp_path)
    assert job._get_path_handlers(job.calibration_job_parameters) == {}


def test_run_calibration_builds_command_and_collects_output(tmp_path, monkeypatch):
    job = _make_job(tmp_path)
    work_folder = job.work_folder
    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8,
        input_json_file="+calibration/calibration/input_v002.json",
    )

    captured = {}

    def mock_call_matlab(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        # The generated user config must exist while MATLAB runs.
        assert (work_folder / USER_CONFIG_FILENAME).exists()
        write_calibration_layer_pair(work_folder, "manual-norm", DATE, 7)

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)

    metadata_path, data_path = job.run_calibration(_handler(7), config)

    # Correct output pair returned.
    assert (
        metadata_path == work_folder / "imap_mag_manual-norm-layer_20260130_v007.json"
    )
    assert (
        data_path == work_folder / "imap_mag_manual-norm-layer-data_20260130_v007.csv"
    )
    assert metadata_path.exists() and data_path.exists()

    # Command carries all the expected arguments in order.
    command = captured["command"]
    assert command.startswith("calibration.scripts.calibrate_l2_offsets(")
    assert "datetime(2026,1,30), datetime(2026,1,30)" in command
    assert ", 8, " in command  # calibration_matrix_version
    assert '"metakernel.txt"' in command
    assert ", 7, " in command  # output_data_version from the handler
    assert '"+calibration/calibration/input_v002.json"' in command
    assert 'modes=["norm"]' in command
    assert "publish_to_sharepoint=false" in command
    assert "display_plots=false" in command

    # Invoked from the repo root with DISPLAY unset and no project path preamble.
    assert captured["kwargs"]["cwd"] == job.matlab_repo_path
    assert captured["kwargs"]["unset_display"] is True
    assert captured["kwargs"]["include_project_paths"] is False

    # The generated user config is cleaned up afterwards.
    assert not (work_folder / USER_CONFIG_FILENAME).exists()


def test_user_config_maps_datastore_and_work_folder(tmp_path, monkeypatch):
    job = _make_job(tmp_path)
    work_folder = job.work_folder
    datastore = job.data_store
    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8, input_json_file="input.json"
    )

    captured_config = {}

    def mock_call_matlab(command, **kwargs):
        captured_config.update(
            json.loads((work_folder / USER_CONFIG_FILENAME).read_text())
        )
        write_calibration_layer_pair(work_folder, "manual-norm", DATE, 1)

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)
    job.run_calibration(_handler(1), config)

    assert captured_config["sharepoint_flight_data"] == str(datastore.resolve())
    assert captured_config["spice_metakernal_root"] == str(datastore.resolve())
    assert captured_config["l2_pre_calibration_outputs"] == str(work_folder.resolve())
    assert captured_config["report_folder"] == str(work_folder.resolve())
    assert captured_config["output_layers_folder"] == str(work_folder.resolve())


def test_generates_metakernel_when_none(tmp_path, monkeypatch):
    job = _make_job(tmp_path, metakernel=None)
    datastore = job.data_store
    work_folder = job.work_folder
    generated_name = "imap_generated_metakernel_v001.tm"
    (datastore / "spice" / "mk" / generated_name).write_text("KERNELS_TO_LOAD = ()")

    def fake_generate(**kwargs):
        return datastore / "spice" / "mk" / generated_name

    # Patched where it is defined, since the job imports it lazily at call time.
    monkeypatch.setattr(
        "imap_mag.cli.fetch.spice.generate_spice_metakernel", fake_generate
    )

    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8, input_json_file="input.json"
    )

    captured = {}

    def mock_call_matlab(command, **kwargs):
        captured["command"] = command
        write_calibration_layer_pair(work_folder, "manual-norm", DATE, 1)

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)
    job.run_calibration(_handler(1), config)

    assert f'"{generated_name}"' in captured["command"]


def test_missing_metakernel_raises(tmp_path, monkeypatch):
    job = _make_job(tmp_path, metakernel="absent.txt", create_metakernel=False)
    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8, input_json_file="input.json"
    )

    monkeypatch.setattr(MODULE_CALL_MATLAB, lambda *a, **k: None)

    with pytest.raises(FileNotFoundError, match=r"absent\.txt"):
        job.run_calibration(_handler(1), config)


def test_missing_output_layer_raises(tmp_path, monkeypatch):
    job = _make_job(tmp_path)
    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8, input_json_file="input.json"
    )

    # call_matlab that produces nothing.
    monkeypatch.setattr(MODULE_CALL_MATLAB, lambda *a, **k: None)

    with pytest.raises(FileNotFoundError, match="was not created"):
        job.run_calibration(_handler(1), config)


def test_wrong_config_type_raises(tmp_path):
    job = _make_job(tmp_path)
    with pytest.raises(TypeError, match="ScriptedL2CalibrationConfig"):
        job.run_calibration(_handler(1), CalibrationConfig())


def test_scripted_calibrate_cli_publishes_layer(
    monkeypatch, temp_datastore, dynamic_work_folder, tmp_path
):
    """End-to-end through the calibrate() CLI with MATLAB mocked out."""
    repo = tmp_path / "repo"
    repo.mkdir()

    def mock_call_matlab(command, **kwargs):
        assert "calibrate_l2_offsets" in command
        write_calibration_layer_pair(dynamic_work_folder, "manual-norm", DATE, 1)

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)

    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8,
        input_json_file="+calibration/calibration/input_v002.json",
    )

    results = calibrate(
        start_date=DATE,
        method=CalibrationMethod.SCRIPTED_L2_CALIBRATION,
        mode=ScienceMode.Normal,
        configuration=config.model_dump_json(),
        matlab_repo_path=repo,
        metakernel=Path("metakernel.txt"),
    )

    assert len(results) == 1
    assert (
        temp_datastore
        / "calibration/layers/2026/01/imap_mag_manual-norm-layer_20260130_v001.json"
    ).exists()
    assert (
        temp_datastore
        / "calibration/layers/2026/01/imap_mag_manual-norm-layer-data_20260130_v001.csv"
    ).exists()
