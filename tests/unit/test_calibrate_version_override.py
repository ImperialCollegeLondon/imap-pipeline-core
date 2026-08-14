"""Tests for the --version-number-override feature in the calibrate CLI.

These tests verify that forcing a specific (major, minor) version:
- Bypasses the normal auto-increment so the layer lands at the requested version.
- Overwrites an existing layer at that version when the content differs.
- Rejects invalid override values early via _validate_version_number_override.
"""

from datetime import datetime
from pathlib import Path

import pytest

from imap_mag.cli.calibrate import _validate_version_number_override, calibrate
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationMethod, LayerDataFormat, Sensor
from tests.util.miscellaneous import write_calibration_layer_pair

MODULE_CALL_MATLAB = (
    "mag_toolkit.calibration.calibrators.GradiometerCalibration.call_matlab"
)
DATE = datetime(2026, 9, 30)


def _cal_work_folder(base: Path, date: datetime, mode: str = "norm") -> Path:
    return base / f"calibrate_{date.strftime('%Y%m%d')}_{mode}"


# ── _validate_version_number_override ─────────────────────────────────────────


@pytest.mark.parametrize(
    "override,error_fragment",
    [
        ((True, 0), "bool"),
        ((0, False), "bool"),
        ((1.5, 0), "integer"),
        ((0, 2.0), "integer"),
        ((-1, 0), "non-negative"),
        ((0, -1), "non-negative"),
        ((1000, 0), "at most 999"),
        ((0, 10000), "at most 9999"),
    ],
)
def test_invalid_override_rejected(override, error_fragment):
    with pytest.raises(ValueError, match=error_fragment):
        _validate_version_number_override(override)


def test_none_override_returns_none():
    assert _validate_version_number_override(None) is None


def test_valid_override_is_returned():
    assert _validate_version_number_override((3, 42)) == (3, 42)


# ── override at the calibrate() level ─────────────────────────────────────────


def test_override_forces_version_no_upversion(
    monkeypatch, temp_datastore, dynamic_work_folder
):
    """With a version override the layer is written at the forced version, not max+1."""
    work = _cal_work_folder(dynamic_work_folder, DATE)

    # Pre-populate v001 in the datastore so auto-increment would produce v002.
    layers_dir = temp_datastore / "calibration/layers/2026/09"
    layers_dir.mkdir(parents=True, exist_ok=True)
    write_calibration_layer_pair(layers_dir, "gradiometer-norm", DATE, 1, seed=1)

    def mock_call_matlab(command):
        write_calibration_layer_pair(work, "gradiometer-norm", DATE, 1, seed=99)

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)

    calibrate(
        start_date=DATE,
        method=CalibrationMethod.GRADIOMETER,
        mode=ScienceMode.Normal,
        sensor=Sensor.MAGO,
        configuration='{"kappa": 0.0, "sc_interference_threshold": 10.0}',
        version_number_override=(1, 1),
        layer_data_format=LayerDataFormat.CSV,
    )

    # The override forces v001.0001; v001.0002 must not exist.
    assert (
        layers_dir / "imap_mag_gradiometer-norm-layer_20260930_v001.0001.json"
    ).exists()
    assert not (
        layers_dir / "imap_mag_gradiometer-norm-layer_20260930_v001.0002.json"
    ).exists()


def test_override_overwrites_layer_content(
    monkeypatch, temp_datastore, dynamic_work_folder
):
    """Content at the forced version is replaced when the new calibration differs."""
    work = _cal_work_folder(dynamic_work_folder, DATE)

    layers_dir = temp_datastore / "calibration/layers/2026/09"
    layers_dir.mkdir(parents=True, exist_ok=True)

    # Existing v001.0005 with seed=0
    write_calibration_layer_pair(layers_dir, "gradiometer-norm", DATE, 5, seed=0)
    existing_json = (
        layers_dir / "imap_mag_gradiometer-norm-layer_20260930_v001.0005.json"
    )
    original_mtime = existing_json.stat().st_mtime

    def mock_call_matlab(command):
        # The override seeds the handler at v005; Gradiometer expects that filename.
        write_calibration_layer_pair(work, "gradiometer-norm", DATE, 5, seed=42)

    monkeypatch.setattr(MODULE_CALL_MATLAB, mock_call_matlab)

    calibrate(
        start_date=DATE,
        method=CalibrationMethod.GRADIOMETER,
        mode=ScienceMode.Normal,
        sensor=Sensor.MAGO,
        configuration='{"kappa": 0.0, "sc_interference_threshold": 10.0}',
        version_number_override=(1, 5),
        layer_data_format=LayerDataFormat.CSV,
    )

    # v005 exists and its content has been replaced (mtime changed).
    assert existing_json.exists()
    assert existing_json.stat().st_mtime != original_mtime
