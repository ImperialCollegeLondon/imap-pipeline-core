"""Unit tests for the generate-leinweber-offsets CLI command.

These tests mock out the actual MATLAB call so they run without MATLAB.
"""

from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.cli.generateLeinweberOffsets import (
    MATLAB_SCRIPT_RELATIVE_PATH,
    ConfigureMidpoint,
    generate_leinweber_offsets,
)
from imap_mag.config import SaveMode
from imap_mag.util import Environment
from mag_toolkit.calibration import Sensor

MODULE_CALL_MATLAB = "imap_mag.cli.generateLeinweberOffsets.call_matlab"
DATE = datetime(2026, 1, 30)


def _make_matlab_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    script = repo / MATLAB_SCRIPT_RELATIVE_PATH
    script.parent.mkdir(parents=True)
    script.write_text("function offset_table = generate_raw_leinweber_offset(); end")
    return repo


def _write_fake_offsets(output_dir: Path, date: datetime = DATE) -> None:
    """Simulate MATLAB writing one CSV per sensor into the output folder."""
    for sensor in ("mago", "magi"):
        name = f"imap_mag_{sensor}-raw-leinweber-offsets-24hr_{date:%Y%m%d}_v001.csv"
        (output_dir / name).write_text(f"offsets,{sensor}\n")


@pytest.fixture
def datastore_env(tmp_path):
    datastore = tmp_path / "datastore"
    datastore.mkdir()
    work_folder = tmp_path / "work"
    with Environment(MAG_DATA_STORE=str(datastore), MAG_WORK_FOLDER=str(work_folder)):
        yield datastore, work_folder


def test_requires_matlab_repo(datastore_env):
    with pytest.raises(ValueError, match="MATLAB repository path is required"):
        generate_leinweber_offsets(date=DATE, matlab_repo=None)


def test_raises_if_matlab_repo_missing(datastore_env, tmp_path):
    with pytest.raises(FileNotFoundError, match="repository not found"):
        generate_leinweber_offsets(date=DATE, matlab_repo=tmp_path / "nope")


def test_raises_if_matlab_script_missing(datastore_env, tmp_path):
    empty_repo = tmp_path / "empty_repo"
    empty_repo.mkdir()
    with pytest.raises(FileNotFoundError, match="generate_raw_leinweber_offset"):
        generate_leinweber_offsets(date=DATE, matlab_repo=empty_repo)


def test_raises_if_no_outputs_produced(datastore_env, tmp_path, monkeypatch):
    repo = _make_matlab_repo(tmp_path)

    def mock_call_matlab(command, **kwargs):
        pass  # MATLAB produces nothing

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)

    with pytest.raises(FileNotFoundError, match="produced no output files"):
        generate_leinweber_offsets(date=DATE, matlab_repo=repo)


def test_generates_and_publishes_offsets(datastore_env, tmp_path, monkeypatch):
    datastore, _ = datastore_env
    repo = _make_matlab_repo(tmp_path)
    captured = {}

    def mock_call_matlab(command, cwd=None, **kwargs):
        captured["command"] = command
        captured["cwd"] = cwd
        import re

        match = re.search(r"'output_folder', \"(?P<path>[^\"]+)\"", command)
        assert match
        _write_fake_offsets(Path(match["path"]))

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)

    saved = generate_leinweber_offsets(
        date=DATE,
        matlab_repo=repo,
        save_mode=SaveMode.LocalOnly,
    )

    assert len(saved) == 2
    for path in saved:
        assert path.is_relative_to(datastore)
        assert path.parent == (
            datastore / "calibration" / "calculated_offsets" / "leinweber"
        )
        assert path.exists()

    names = sorted(p.name for p in saved)
    assert names == [
        "imap_mag_magi-raw-leinweber-offsets-24hr_20260130_v001.csv",
        "imap_mag_mago-raw-leinweber-offsets-24hr_20260130_v001.csv",
    ]

    command = captured["command"]
    assert command.startswith("calibration.tools.generate_raw_leinweber_offset(")
    # date is passed straight through - MATLAB derives the analysis midpoint
    # itself, it is not computed here.
    assert "datetime(2026,1,30,0,0,0)" in command
    assert '"MAGo", "MAGi"' in command
    assert "'calibration_matrix_version', 9" in command
    assert "'verify', true" in command
    assert "configure_midpoint" not in command
    assert "'allow_pivot_platform_extrapolation', false" in command


def test_builds_command_with_overrides(datastore_env, tmp_path, monkeypatch):
    repo = _make_matlab_repo(tmp_path)
    captured = {}

    def mock_call_matlab(command, **kwargs):
        captured["command"] = command
        import re

        match = re.search(r"'output_folder', \"(?P<path>[^\"]+)\"", command)
        _write_fake_offsets(Path(match["path"]))

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)

    generate_leinweber_offsets(
        date=DATE,
        matlab_repo=repo,
        duration_hours=12,
        sensors=[Sensor.MAGO],
        calibration_matrix_version=7,
        verify=False,
        configure_midpoint=ConfigureMidpoint.USE_MID_DAY,
        allow_pivot_platform_extrapolation=True,
        save_mode=SaveMode.LocalOnly,
    )

    command = captured["command"]
    assert "datetime(2026,1,30,0,0,0)" in command
    assert '["MAGo"]' in command
    assert "'calibration_matrix_version', 7" in command
    assert "'verify', false" in command
    assert "'configure_midpoint', \"USE_MID_DAY\"" in command
    assert "'allow_pivot_platform_extrapolation', true" in command
