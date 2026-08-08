"""Unit tests for the scripted L2 calibration job and its CLI wiring.

These tests mock out the actual MATLAB call so they run without MATLAB.
"""

import json
from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.cli.calibrate import calibrate
from imap_mag.config import AppSettings, GradiometryConfig
from imap_mag.io.file import CalculatedOffsetsPathHandler, CalibrationLayerPathHandler
from imap_mag.io.file.CalculatedOffsetsPathHandler import OFFSET_TYPES
from imap_mag.io.file.IFilePathHandler import IFilePathHandler
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import (
    CalibrationJobParameters,
    CalibrationMethod,
    CreateOffsets,
    DatastoreAccessMode,
    Sensor,
)
from mag_toolkit.calibration.CalibrationConfig import (
    ScriptedL2CalibrationConfig,
)
from mag_toolkit.calibration.CalibrationLayer import CalibrationLayer
from mag_toolkit.calibration.calibrators.ScriptedL2Calibration import (
    OUTPUT_SUBFOLDER_NAME,
    SPARSE_DATASTORE_FOLDER_NAME,
    USER_CONFIG_FILENAME,
    ScriptedL2CalibrationJob,
)
from tests.util.miscellaneous import write_calibration_layer_pair

MODULE_CALL_MATLAB = (
    "mag_toolkit.calibration.calibrators.ScriptedL2Calibration.call_matlab"
)
DATE = datetime(2026, 1, 30)


def _make_matlab_repo(tmp_path: Path) -> Path:
    """Create a fake MATLAB repo containing the expected entry-point script."""
    repo = tmp_path / "repo"
    script = repo / "+calibration" / "+scripts" / "calibrate_l2_offsets.m"
    script.parent.mkdir(parents=True)
    script.write_text("function calibrate_l2_offsets(); end")
    return repo


def _make_app_settings(
    tmp_path: Path, metakernel: str | None = "metakernel.txt"
) -> Path:
    """Create a temp datastore (with metakernel) and return its path."""
    datastore = tmp_path / "datastore"
    (datastore / "spice" / "mk").mkdir(parents=True)
    if metakernel:
        (datastore / "spice" / "mk" / metakernel).write_text(
            "\\begindata\nPATH_VALUES = ( 'spice' )\nPATH_SYMBOLS = ( 'KERNELS' )\n"
            "KERNELS_TO_LOAD = ( '$KERNELS/lsk/naif0012.tls' )\n"
        )
    return datastore


def _make_job(
    tmp_path: Path,
    metakernel: str | None = "metakernel.txt",
    create_metakernel: bool = True,
) -> ScriptedL2CalibrationJob:
    """Build a ScriptedL2CalibrationJob wired to temp folders.

    Work folder, repo and datastore are reachable via the returned job's
    ``work_folder``, ``matlab_repo_path`` and ``data_store`` attributes.
    """
    datastore = _make_app_settings(tmp_path, metakernel if create_metakernel else None)
    repo = _make_matlab_repo(tmp_path)
    app_settings = AppSettings(data_store=datastore, work_folder=tmp_path / "work")

    params = CalibrationJobParameters(
        date=DATE, mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    job = ScriptedL2CalibrationJob(
        params,
        app_settings,
        matlab_repo_path=repo,
        metakernel=metakernel,
    )
    job.setup(datastore)
    return job


def _handler(version: int) -> CalibrationLayerPathHandler:
    return CalibrationLayerPathHandler(
        descriptor="manual-norm", content_date=DATE, version=version
    )


def _write_work_offsets(
    output_dir: Path, date: datetime = DATE, tag: str = "a"
) -> None:
    """Simulate MATLAB writing the four spin-plane offset CSVs into the output dir.

    MATLAB writes offsets directly under ``output_dir/<offset_type>/``.
    One CSV per sensor (mago/magi) per offset type (spin_plane/spin_optimised);
    the per-type file name is derived from the path handler so it matches production.
    Content is tagged so tests can force identical vs changed content.
    """
    for offset_type in OFFSET_TYPES:
        folder = output_dir / offset_type
        folder.mkdir(parents=True, exist_ok=True)
        for sensor in ("mago", "magi"):
            handler = CalculatedOffsetsPathHandler(
                sensor=sensor, offset_type=offset_type, content_date=date, version=1
            )
            (folder / handler.get_filename()).write_text(
                f"offsets,{sensor},{offset_type},{tag}\n"
            )


def test_requires_matlab_repo_path(tmp_path):
    datastore = _make_app_settings(tmp_path)
    app_settings = AppSettings(data_store=datastore, work_folder=tmp_path / "work")
    params = CalibrationJobParameters(
        date=DATE, mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    with pytest.raises(ValueError, match="repository path is required"):
        ScriptedL2CalibrationJob(params, app_settings, matlab_repo_path=None)


def test_raises_if_matlab_repo_missing(tmp_path):
    datastore = _make_app_settings(tmp_path)
    app_settings = AppSettings(data_store=datastore, work_folder=tmp_path / "work")
    params = CalibrationJobParameters(
        date=DATE, mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    with pytest.raises(FileNotFoundError, match="repository not found"):
        ScriptedL2CalibrationJob(
            params, app_settings, matlab_repo_path=tmp_path / "nope"
        )


def test_raises_if_matlab_script_missing(tmp_path):
    datastore = _make_app_settings(tmp_path)
    app_settings = AppSettings(data_store=datastore, work_folder=tmp_path / "work")
    empty_repo = tmp_path / "empty_repo"
    empty_repo.mkdir()
    params = CalibrationJobParameters(
        date=DATE, mode=ScienceMode.Normal, sensor=Sensor.MAGO
    )
    with pytest.raises(FileNotFoundError, match="calibrate_l2_offsets"):
        ScriptedL2CalibrationJob(params, app_settings, matlab_repo_path=empty_repo)


def test_work_folder_is_dynamic(tmp_path):
    job = _make_job(tmp_path)
    assert job.work_folder.name == "calibrate_20260130_norm"


def test_no_science_files_are_fetched(tmp_path):
    job = _make_job(tmp_path)
    assert job._get_path_handlers(job.calibration_job_parameters) == {}


def test_run_calibration_builds_command_and_collects_output(tmp_path, monkeypatch):
    job = _make_job(tmp_path)
    work_folder = job.work_folder
    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8,
        input_json_file="+calibration/calibration/input_v002.json",
        datastore_access_mode=DatastoreAccessMode.READ_DIRECTLY,
        matlab_repo=str(job.matlab_repo_path),
    )

    captured = {}

    def mock_call_matlab(command, **kwargs):
        captured["command"] = command
        captured["kwargs"] = kwargs
        assert (work_folder / USER_CONFIG_FILENAME).exists()
        write_calibration_layer_pair(
            work_folder / OUTPUT_SUBFOLDER_NAME, "manual-norm", DATE, 7
        )

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)

    returned = job.run_calibration(_handler(7), config)
    output_dir = work_folder / OUTPUT_SUBFOLDER_NAME
    json_file = output_dir / "imap_mag_manual-norm-layer_20260130_v001.0007.json"
    csv_file = output_dir / "imap_mag_manual-norm-layer-data_20260130_v001.0007.csv"
    job.cleanup()

    assert json_file in returned
    assert csv_file in returned
    assert json_file.exists() and csv_file.exists()

    command = captured["command"]
    assert command.startswith("calibration.scripts.calibrate_l2_offsets(")
    assert "datetime(2026,1,30), datetime(2026,1,30)" in command
    assert ", 8, " in command
    assert '"metakernel.txt"' in command
    assert "[0, 7]" in command
    assert '"+calibration/calibration/input_v002.json"' in command
    assert 'modes=["norm"]' in command
    assert "publish_to_sharepoint=false" in command
    assert "display_plots=false" in command

    # Invoked from the repo root, no project path preamble.
    assert captured["kwargs"]["cwd"] == job.matlab_repo_path
    assert captured["kwargs"]["include_project_paths"] is False
    # Norm-mode per-day timeout from config.
    assert captured["kwargs"]["timeout"] == 600

    assert not (work_folder / USER_CONFIG_FILENAME).exists()


def test_burst_mode_uses_burst_timeout(tmp_path, monkeypatch):
    datastore = _make_app_settings(tmp_path)
    repo = _make_matlab_repo(tmp_path)
    app_settings = AppSettings(data_store=datastore, work_folder=tmp_path / "work")
    params = CalibrationJobParameters(
        date=DATE, mode=ScienceMode.Burst, sensor=Sensor.MAGO
    )
    job = ScriptedL2CalibrationJob(
        params, app_settings, matlab_repo_path=repo, metakernel="metakernel.txt"
    )
    job.setup(datastore)
    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8,
        input_json_file="input.json",
        datastore_access_mode=DatastoreAccessMode.READ_DIRECTLY,
        matlab_repo=str(job.matlab_repo_path),
    )

    captured = {}

    def mock_call_matlab(command, **kwargs):
        captured["kwargs"] = kwargs
        write_calibration_layer_pair(
            job.work_folder / OUTPUT_SUBFOLDER_NAME, "manual-burst", DATE, 1
        )

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)
    job.run_calibration(
        CalibrationLayerPathHandler(
            descriptor="manual-burst", content_date=DATE, version=1
        ),
        config,
    )
    assert captured["kwargs"]["timeout"] == 3600


def test_user_config_maps_datastore_and_work_folder(tmp_path, monkeypatch):
    job = _make_job(tmp_path)
    work_folder = job.work_folder
    datastore = job.data_store
    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8,
        input_json_file="input.json",
        datastore_access_mode=DatastoreAccessMode.READ_DIRECTLY,
        matlab_repo=str(job.matlab_repo_path),
    )

    captured_config = {}

    def mock_call_matlab(command, **kwargs):
        captured_config.update(
            json.loads((work_folder / USER_CONFIG_FILENAME).read_text())
        )
        write_calibration_layer_pair(
            work_folder / OUTPUT_SUBFOLDER_NAME, "manual-norm", DATE, 1
        )

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)
    job.run_calibration(_handler(1), config)

    output_dir = work_folder / OUTPUT_SUBFOLDER_NAME
    assert captured_config["sharepoint_flight_data"] == str(datastore.resolve())
    assert captured_config["spice_metakernal_root"] == str(datastore.resolve())
    assert captured_config["l2_pre_calibration_outputs"] == str(output_dir.resolve())
    assert (
        captured_config["report_folder"]
        == str(datastore.resolve()) + "/calibration/reports"
    )
    assert captured_config["output_layers_folder"] == str(output_dir.resolve())


def test_local_work_folder_copy_builds_sparse_datastore(tmp_path, monkeypatch):
    job = _make_job(
        tmp_path,
    )
    work_folder = job.work_folder
    datastore = job.data_store
    # Add a referenced kernel so the sparse builder has something to copy.
    (datastore / "spice" / "lsk").mkdir(parents=True)
    (datastore / "spice" / "lsk" / "naif0012.tls").write_text("kernel")
    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8,
        input_json_file="input.json",
        datastore_access_mode=DatastoreAccessMode.LOCAL_WORK_FOLDER_COPY,
        matlab_repo=str(job.matlab_repo_path),
    )

    captured = {}

    def mock_call_matlab(command, **kwargs):
        captured_config = json.loads((work_folder / USER_CONFIG_FILENAME).read_text())
        captured["sharepoint"] = captured_config["sharepoint_flight_data"]
        sparse_root = work_folder / SPARSE_DATASTORE_FOLDER_NAME
        # The sparse datastore exists during the run with the metakernel + kernel copied.
        captured["mk_copied"] = (
            sparse_root / "spice" / "mk" / "metakernel.txt"
        ).exists()
        captured["kernel_copied"] = (
            sparse_root / "spice" / "lsk" / "naif0012.tls"
        ).exists()
        write_calibration_layer_pair(
            work_folder / OUTPUT_SUBFOLDER_NAME, "manual-norm", DATE, 1
        )

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)
    job.run_calibration(_handler(1), config)
    job.cleanup()

    sparse_root = work_folder / SPARSE_DATASTORE_FOLDER_NAME
    # MATLAB was pointed at the sparse copy, which held the copied SPICE files...
    assert captured["sharepoint"] == str(sparse_root.resolve())
    assert captured["mk_copied"] is True
    assert captured["kernel_copied"] is True
    # ...and the sparse copy is cleaned up afterwards.
    assert not sparse_root.exists()


def test_missing_metakernel_raises(tmp_path, monkeypatch):
    job = _make_job(tmp_path, metakernel="absent.txt", create_metakernel=False)
    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8,
        input_json_file="input.json",
        matlab_repo=str(job.matlab_repo_path),
        datastore_access_mode=DatastoreAccessMode.READ_DIRECTLY,
    )
    monkeypatch.setattr(MODULE_CALL_MATLAB, lambda *a, **k: None)
    with pytest.raises(FileNotFoundError, match=r"absent\.txt"):
        job.run_calibration(_handler(1), config)


def test_missing_output_layer_raises(tmp_path, monkeypatch):
    job = _make_job(tmp_path)
    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8,
        input_json_file="input.json",
        matlab_repo=str(job.matlab_repo_path),
    )
    monkeypatch.setattr(MODULE_CALL_MATLAB, lambda *a, **k: None)
    with pytest.raises(FileNotFoundError, match="no output files"):
        job.run_calibration(_handler(1), config)


def test_wrong_config_type_raises(tmp_path):
    job = _make_job(tmp_path)
    with pytest.raises(TypeError, match="ScriptedL2CalibrationConfig"):
        job.run_calibration(_handler(1), GradiometryConfig())


def test_generates_metakernel_when_none(tmp_path, monkeypatch):
    job = _make_job(tmp_path, metakernel=None)
    datastore = job.data_store
    work_folder = job.work_folder
    generated_name = "imap_generated_metakernel_v001.tm"
    (datastore / "spice" / "mk" / generated_name).write_text("KERNELS_TO_LOAD = ()")

    def fake_generate(**kwargs):
        return datastore / "spice" / "mk" / generated_name

    monkeypatch.setattr(
        "imap_mag.cli.fetch.spice.generate_spice_metakernel", fake_generate
    )

    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8,
        input_json_file="input.json",
        matlab_repo=str(job.matlab_repo_path),
    )

    captured = {}

    def mock_call_matlab(command, **kwargs):
        captured["command"] = command
        write_calibration_layer_pair(
            work_folder / OUTPUT_SUBFOLDER_NAME, "manual-norm", DATE, 1
        )

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)
    job.run_calibration(_handler(1), config)

    assert f'"{generated_name}"' in captured["command"]


def test_layer_data_hash_algorithm_matches_matlab_contract(tmp_path):
    """Pin the exact data_hash algorithm shared with the MATLAB calibration.

    The MATLAB ``calibrate_l2_offsets`` pipeline now writes the layer JSON
    ``data_hash`` itself, and imap-pipeline-core trusts that value instead of
    recomputing it. Both sides must therefore agree byte-for-byte: the hash is the
    lowercase hex MD5 digest of the raw CSV file bytes. The identical fixed content
    and expected digest are asserted in the calibration repo's tLayer MATLAB test.
    """
    content = (
        b"time,offset_x,offset_y,offset_z,timedelta,quality_flag,quality_bitmask\n"
        b"2026-01-30T00:00:00.000000000,1,2,3,0,0,0\n"
    )
    csv_file = tmp_path / "sample-layer-data.csv"
    csv_file.write_bytes(content)

    assert (
        IFilePathHandler.default_file_hash(csv_file)
        == "8e6ec03138bae9a6d3521d46ec6c30ec"
    )


def test_python_preserves_matlab_supplied_data_hash(tmp_path):
    """A data_hash already present in the layer metadata is not recomputed.

    MATLAB fills in ``data_hash`` when it writes the layer, so re-saving the layer
    in the pipeline (``save_calibration_layer``) must keep MATLAB's value rather than
    overwrite it from the CSV on disk.
    """
    json_path, _ = write_calibration_layer_pair(tmp_path, "manual-norm", DATE, 1)
    matlab_hash = "deadbeefdeadbeefdeadbeefdeadbeef"
    layer = CalibrationLayer.from_file(json_path, load_contents=False)
    layer.metadata.data_hash = matlab_hash
    layer.save_calibration_layer(json_path, createDirectory=False, save_contents=False)

    reloaded = CalibrationLayer.from_file(json_path, load_contents=False)
    assert reloaded.metadata.data_hash == matlab_hash


def test_scripted_calibrate_cli_publishes_layer(
    monkeypatch, temp_datastore, dynamic_work_folder, tmp_path
):
    """End-to-end through the calibrate() CLI with MATLAB mocked out."""
    repo = _make_matlab_repo(tmp_path)
    work_folder = dynamic_work_folder / "calibrate_20260130_norm"

    def mock_call_matlab(command, **kwargs):
        assert "calibrate_l2_offsets" in command
        write_calibration_layer_pair(
            work_folder / OUTPUT_SUBFOLDER_NAME, "manual-norm", DATE, 1
        )

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)

    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8,
        input_json_file="+calibration/calibration/input_v002.json",
        matlab_repo=str(repo),
    )

    results = calibrate(
        start_date=DATE,
        method=CalibrationMethod.SCRIPTED_L2_CALIBRATION,
        mode=ScienceMode.Normal,
        configuration=config.model_dump_json(),
        metakernel=Path("metakernel.txt"),
    )

    results = [f for f in results if not "metakernel.txt" not in str(f)]

    assert len(results) == 1
    assert (
        temp_datastore
        / "calibration/layers/2026/01/imap_mag_manual-norm-layer_20260130_v001.0001.json"
    ).exists()
    assert (
        temp_datastore
        / "calibration/layers/2026/01/imap_mag_manual-norm-layer-data_20260130_v001.0001.csv"
    ).exists()


def test_run_calibration_collects_files_in_offset_directories(tmp_path, monkeypatch):
    job = _make_job(tmp_path)
    work_folder = job.work_folder
    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8,
        input_json_file="input.json",
        matlab_repo=str(job.matlab_repo_path),
    )

    captured = {}

    def mock_call_matlab(command, **kwargs):
        captured["command"] = command
        captured["config"] = json.loads(
            (work_folder / USER_CONFIG_FILENAME).read_text()
        )
        output_dir = work_folder / OUTPUT_SUBFOLDER_NAME
        write_calibration_layer_pair(output_dir, "manual-norm", DATE, 1)
        _write_work_offsets(output_dir)

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)
    items = job.run_calibration(_handler(1), config)

    output_dir = work_folder / OUTPUT_SUBFOLDER_NAME
    assert captured["config"]["output_offsets_folder"] == str(output_dir.resolve())
    # Six files total: layer JSON + CSV + 4 offsets (mago/magi x spin_plane/spin_optimised).
    assert len(items) == 6
    assert {f.parent.name for f in items if f.parent.name in OFFSET_TYPES} == set(
        OFFSET_TYPES
    )
    expected = {
        "outputs/imap_mag_manual-norm-layer-data_20260130_v001.0001.csv",
        "outputs/imap_mag_manual-norm-layer_20260130_v001.0001.json",
        "outputs/spin_optimised/imap_mag_magi-spin-plane-optimised-offsets_20260130_v001.csv",
        "outputs/spin_optimised/imap_mag_mago-spin-plane-optimised-offsets_20260130_v001.csv",
        "outputs/spin_plane/imap_mag_magi-spin-plane-offsets_20260130_v001.csv",
        "outputs/spin_plane/imap_mag_mago-spin-plane-offsets_20260130_v001.csv",
    }
    assert {str(f.relative_to(work_folder)) for f in items} == expected


def _run_scripted_cli_with_offsets(monkeypatch, work_folder: Path, tag: str) -> None:
    """Run the calibrate() CLI for the scripted-L2 method with offsets enabled."""

    def mock_call_matlab(command, **kwargs):
        output_dir = work_folder / OUTPUT_SUBFOLDER_NAME
        write_calibration_layer_pair(output_dir, "manual-norm", DATE, 1)
        _write_work_offsets(output_dir, tag=tag)

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)

    repo = _make_matlab_repo(work_folder.parent / f"repo_{tag}")
    config = ScriptedL2CalibrationConfig(
        calibration_matrix_version=8,
        input_json_file="+calibration/calibration/input_v002.json",
        matlab_repo=str(repo),
    )
    calibrate(
        start_date=DATE,
        method=CalibrationMethod.SCRIPTED_L2_CALIBRATION,
        mode=ScienceMode.Normal,
        configuration=config.model_dump_json(),
        metakernel=Path("metakernel.txt"),
    )


def _offsets_path(datastore: Path, offset_type: str, sensor: str, version: int) -> Path:
    handler = CalculatedOffsetsPathHandler(
        sensor=sensor, offset_type=offset_type, content_date=DATE, version=version
    )
    return handler.get_full_path(datastore)


def test_scripted_calibrate_cli_publishes_offsets(
    monkeypatch, temp_datastore, dynamic_work_folder, tmp_path
):
    """First run publishes the four offset CSVs at v001 in the datastore."""
    work_folder = dynamic_work_folder / "calibrate_20260130_norm"
    _run_scripted_cli_with_offsets(monkeypatch, work_folder, tag="a")

    for offset_type in ("spin_plane", "spin_optimised"):
        for sensor in ("mago", "magi"):
            assert _offsets_path(temp_datastore, offset_type, sensor, 1).exists()

    # Pin the exact (unique) file names for each product.
    base = temp_datastore / "calibration/calculated_offsets"
    assert (
        base / "spin_plane/imap_mag_mago-spin-plane-offsets_20260130_v001.csv"
    ).exists()
    assert (
        base
        / "spin_optimised/imap_mag_mago-spin-plane-optimised-offsets_20260130_v001.csv"
    ).exists()


def test_scripted_calibrate_cli_upversions_changed_offsets(
    monkeypatch, temp_datastore, dynamic_work_folder, tmp_path
):
    """Different content vs the latest datastore offsets is written as a new version."""
    # Seed the datastore with a v001 that differs from what the run will produce.
    for offset_type in ("spin_plane", "spin_optimised"):
        for sensor in ("mago", "magi"):
            existing = _offsets_path(temp_datastore, offset_type, sensor, 1)
            existing.parent.mkdir(parents=True, exist_ok=True)
            existing.write_text("pre-existing different content\n")

    work_folder = dynamic_work_folder / "calibrate_20260130_norm"
    _run_scripted_cli_with_offsets(monkeypatch, work_folder, tag="new")

    for offset_type in ("spin_plane", "spin_optimised"):
        for sensor in ("mago", "magi"):
            assert _offsets_path(temp_datastore, offset_type, sensor, 1).exists()
            assert _offsets_path(temp_datastore, offset_type, sensor, 2).exists()


def test_scripted_calibrate_cli_reuses_identical_offsets(
    monkeypatch, temp_datastore, dynamic_work_folder, tmp_path
):
    """Identical content to the latest datastore offsets does not create a new version."""
    # Seed the datastore v001 with byte-identical content to what the run produces.
    for offset_type in ("spin_plane", "spin_optimised"):
        for sensor in ("mago", "magi"):
            existing = _offsets_path(temp_datastore, offset_type, sensor, 1)
            existing.parent.mkdir(parents=True, exist_ok=True)
            existing.write_text(f"offsets,{sensor},{offset_type},same\n")

    work_folder = dynamic_work_folder / "calibrate_20260130_norm"
    _run_scripted_cli_with_offsets(monkeypatch, work_folder, tag="same")

    for offset_type in ("spin_plane", "spin_optimised"):
        for sensor in ("mago", "magi"):
            assert _offsets_path(temp_datastore, offset_type, sensor, 1).exists()
            assert not _offsets_path(temp_datastore, offset_type, sensor, 2).exists()


class TestCreateOffsets:
    """write_offsets flag: automatic default and explicit override."""

    def _run_and_capture_command(
        self,
        tmp_path,
        monkeypatch,
        mode: ScienceMode,
        write_offsets: CreateOffsets = CreateOffsets.AUTOMATIC,
    ) -> str:
        """Run the scripted-L2 job for the given mode and return the captured MATLAB command."""
        datastore = _make_app_settings(tmp_path)
        repo = _make_matlab_repo(tmp_path)
        app_settings = AppSettings(data_store=datastore, work_folder=tmp_path / "work")
        params = CalibrationJobParameters(date=DATE, mode=mode, sensor=Sensor.MAGO)
        job = ScriptedL2CalibrationJob(
            params, app_settings, matlab_repo_path=repo, metakernel="metakernel.txt"
        )
        job.setup(datastore)

        config = ScriptedL2CalibrationConfig(
            calibration_matrix_version=8,
            input_json_file="input.json",
            matlab_repo=str(job.matlab_repo_path),
            write_offsets=write_offsets,
        )

        captured = {}

        def mock_call_matlab(command, **kw):
            captured["command"] = command
            write_calibration_layer_pair(
                job.work_folder / OUTPUT_SUBFOLDER_NAME, "manual-norm", DATE, 1
            )

        monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)
        handler = CalibrationLayerPathHandler(
            descriptor="manual-norm", content_date=DATE, version=1
        )
        job.run_calibration(handler, config)
        return captured["command"]

    def test_automatic_norm_mode_enables_write_offsets(self, tmp_path, monkeypatch):
        """Automatic mode passes write_offsets=true for normal mode."""
        command = self._run_and_capture_command(
            tmp_path, monkeypatch, ScienceMode.Normal, CreateOffsets.AUTOMATIC
        )
        assert "write_offsets=true" in command

    def test_automatic_burst_mode_disables_write_offsets(self, tmp_path, monkeypatch):
        """Automatic mode passes write_offsets=false for burst mode."""
        command = self._run_and_capture_command(
            tmp_path, monkeypatch, ScienceMode.Burst, CreateOffsets.AUTOMATIC
        )
        assert "write_offsets=false" in command

    def test_yes_overrides_burst_default(self, tmp_path, monkeypatch):
        """CreateOffsets.YES forces write_offsets=true even for burst mode."""
        command = self._run_and_capture_command(
            tmp_path, monkeypatch, ScienceMode.Burst, CreateOffsets.ALWAYS
        )
        assert "write_offsets=true" in command

    def test_no_overrides_norm_default(self, tmp_path, monkeypatch):
        """CreateOffsets.NO forces write_offsets=false even for normal mode."""
        command = self._run_and_capture_command(
            tmp_path, monkeypatch, ScienceMode.Normal, CreateOffsets.NEVER
        )
        assert "write_offsets=false" in command
