import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from shutil import which

import numpy as np
import pandas as pd
import pytest
from spacepy import pycdf

from imap_mag.cli.apply import CalibrationApplicator, apply
from imap_mag.cli.calibrate import calibrate, gradiometry
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import (
    CalibrationLayer,
    CalibrationMethod,
    CalibrationValue,
    ScienceValue,
    Sensor,
)
from mag_toolkit.calibration.MatlabWrapper import setup_matlab_path
from tests.util.miscellaneous import (  # noqa: F401
    DATASTORE,
    create_test_file,
    temp_datastore,
    tidyDataFolders,
)


def prepare_test_file(test_file, sub_folders, year=None, month=None, rename=None):
    """
    Prepare a calibration test file by copying the specified calibration layer
    to the output directory with the correct folder structure.
    """
    dest_filename = test_file if rename is None else rename
    test_datastore = Path(os.getenv("MAG_DATA_STORE", "output"))
    if year and month:
        dest_filepath = (
            test_datastore / sub_folders / str(year) / f"{month:02d}" / dest_filename
        )
        original_filepath = DATASTORE / f"{sub_folders}/{year}/{month:02d}/{test_file}"
    else:
        dest_filepath = test_datastore / sub_folders / dest_filename
        original_filepath = DATASTORE / sub_folders / test_file
    os.makedirs(dest_filepath.parent, exist_ok=True)
    shutil.copy(
        original_filepath,
        dest_filepath,
    )


def test_apply_produces_output_science_file_and_offsets_file_with_data(
    tmp_path,
    temp_datastore,  # noqa: F811
):
    apply(
        layers=["imap_mag_noop-layer-meta_20251017_v001.json"],
        input="imap_mag_l1c_norm-mago_20251017_v001.cdf",
        date=datetime(2025, 10, 17),
    )
    output_l2_file = (
        temp_datastore
        / "science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-mago_20251017_v000.cdf"
    )
    assert output_l2_file.exists()
    output_offsets_file = (
        temp_datastore
        / "science-ancillary/l2-offsets/2025/10/imap_mag_l2-norm-offsets_20251017_20251017_v000.cdf"
    )
    assert output_offsets_file.exists()

    with pycdf.CDF(str(output_offsets_file)) as offsets_cdf:
        assert "offsets" in offsets_cdf
        assert "epoch" in offsets_cdf
        assert "timedeltas" in offsets_cdf
        assert "quality_flag" in offsets_cdf
        assert "quality_bitmask" in offsets_cdf

    with pycdf.CDF(str(output_l2_file)) as cdf:
        assert "vectors" in cdf
        assert "epoch" in cdf
        assert "magnitude" in cdf
        assert "quality_flags" in cdf
        assert "quality_bitmask" in cdf


def test_apply_fails_when_timestamps_dont_align(tmp_path, temp_datastore):  # noqa: F811
    with pytest.raises(Exception, match="Layer and data timestamps do not align"):
        apply(
            layers=["imap_mag_misaligned-timestamps-layer-meta_20251017_v001.json"],
            input="imap_mag_l1c_norm-mago_20251017_v001.cdf",
            date=datetime(2025, 10, 17),
        )

    assert not (
        temp_datastore
        / "science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-mago_20251017_v001.cdf"
    ).exists()
    assert not (
        temp_datastore
        / "science-ancillary/l2-offsets/2025/10/imap_mag_l2-norm-offsets_20251017_20251017_v001.cdf"
    ).exists()


def test_apply_fails_when_no_layers_provided(tmp_path, temp_datastore):  # noqa: F811
    # No layers provided, should raise ValueError
    with pytest.raises(
        ValueError, match="No calibration layers or rotation file provided."
    ):
        apply(
            layers=[],
            input="imap_mag_l1c_norm-mago_20251017_v001.cdf",
            date=datetime(2025, 10, 17),
        )

    assert not (
        temp_datastore
        / "science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-mago_20251017_v000.cdf"
    ).exists()
    assert not (
        temp_datastore
        / "science-ancillary/l2-offsets/2025/10/imap_mag_l2_norm-offsets_20251017_20251017_v000.cdf"
    ).exists()


def test_apply_performs_correct_rotation(tmp_path):
    prepare_test_file(
        "imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        "science/mag/l1c",
        2025,
        10,
        rename="imap_mag_l1c_norm-mago_20251017_v000.cdf",
    )
    prepare_test_file(
        "imap_mag_l2-calibration_20251017_v004.cdf",
        "science-ancillary/l2-rotation",
    )
    apply(
        layers=[],
        input="imap_mag_l1c_norm-mago_20251017_v000.cdf",
        rotation=Path("imap_mag_l2-calibration_20251017_v004.cdf"),
        date=datetime(2025, 10, 17),
    )

    output_file = (
        "output/science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-mago_20251017_v000.cdf"
    )

    assert Path(output_file).exists()

    # Correct vectors calculated manually using MATLAB
    correct_vecs = [
        [-7351.97046454686, -25221.0251719171, -36006.8993],
        [-8847.56935760261, -25370.1800953166, -36000.26330704],
        [-9476.9412947865, -23426.8605232176, -36124.2844555925],
        [-9654.64764180912, -27472.6841916782, -35709.9103366105],
    ]

    with pycdf.CDF(output_file) as cdf:
        vectors = cdf["vectors"][...]
        for correct_vec, vec in zip(correct_vecs, vectors):  # type: ignore
            # Convert to list for comparison
            assert vec == pytest.approx(correct_vec, rel=1e-6)


def test_apply_adds_offsets_together_correctly(tmp_path, temp_datastore):  # noqa: F811
    prepare_test_file(
        "imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        "science/mag/l1c",
        2025,
        10,
        rename="imap_mag_l1c_norm-mago_20251017_v000.cdf",
    )
    apply(
        layers=["imap_mag_four-vector-offsets-layer-meta_20251017_v001.json"],
        input="imap_mag_l1c_norm-mago_20251017_v000.cdf",
        date=datetime(2025, 10, 17),
    )

    output_file = (
        temp_datastore
        / "science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-mago_20251017_v000.cdf"
    )

    assert output_file.exists()

    # Correct vectors calculated manually using MATLAB
    correct_vecs = [
        [-9783.16736, -23613.88873, -35999.27727],
        [-9792.60397, -23604.93445, -35985.69583],
        [-9785.58375, -23603.11448, -36006.90274],
        [-9784.77406, -23584.60784, -36042.23171],
    ]

    with pycdf.CDF(str(output_file)) as cdf:
        vectors = cdf["vectors"][...]
        for correct_vec, vec in zip(correct_vecs, vectors):  # type: ignore
            # Convert to list for comparison
            assert vec == pytest.approx(correct_vec, rel=1e-6)


def test_simple_interpolation_calibration_values_apply_correctly():
    calibration_values = [
        CalibrationValue(time=np.datetime64("2025-01-01T12:30"), value=[0, 0, 0]),
        CalibrationValue(time=np.datetime64("2025-01-01T12:30:02"), value=[2, 0, 0]),
    ]
    science_values = [
        ScienceValue(
            time=np.datetime64("2025-01-01T12:30:01"), value=[0, 0, 0], range=3
        )
    ]

    applier = CalibrationApplicator()

    resulting_science, resulting_calibration = (
        applier._apply_interpolation_points_to_science_values(
            science_values, calibration_values
        )
    )

    assert resulting_science[0].time == resulting_calibration[0].time
    assert len(resulting_science) == len(resulting_calibration)
    assert resulting_calibration[0].value == [1, 0, 0]
    assert resulting_science[0].value == [1, 0, 0]


def test_apply_writes_magnitudes_correctly(tmp_path, temp_datastore):  # noqa: F811
    apply(
        layers=["imap_mag_four-vector-offsets-layer-meta_20251017_v001.json"],
        input="imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        date=datetime(2025, 10, 17),
    )

    output_file = (
        temp_datastore
        / "science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-mago-four-vectors-four-ranges_20251017_v000.cdf"
    )

    assert output_file.exists()

    # Correct vectors calculated manually using MATLAB
    correct_vecs = [
        [-9783.16736, -23613.88873, -35999.27727],
        [-9792.60397, -23604.93445, -35985.69583],
        [-9785.58375, -23603.11448, -36006.90274],
        [-9784.77406, -23584.60784, -36042.23171],
    ]

    with pycdf.CDF(str(output_file)) as cdf:
        magnitudes = cdf["magnitude"][...]
        for correct_vec, mag in zip(correct_vecs, magnitudes):  # type: ignore
            # Convert to list for comparison
            computed_magnitude = np.linalg.norm(correct_vec)
            assert mag == pytest.approx(computed_magnitude, rel=1e-6)


def get_test_matlab_command():
    if os.getenv("MLM_LICENSE_TOKEN") and (which("matlab-batch") is not None):
        return "matlab-batch"
    else:
        return "matlab"


@pytest.fixture()
def matlab_test_setup():
    setup_matlab_path("src/matlab", get_test_matlab_command())
    yield


@pytest.mark.skipif(
    not (os.getenv("MLM_LICENSE_FILE") or os.getenv("MLM_LICENSE_TOKEN"))
    or which(get_test_matlab_command()) is None,
    reason="MATLAB License not set or MATLAB is not available; skipping MATLAB tests",
)
def test_empty_calibration_layer_is_created_with_offsets_for_every_vector(
    matlab_test_setup,
    tmp_path,
    monkeypatch,
    temp_datastore,  # noqa: F811
):
    monkeypatch.setattr(
        "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
        get_test_matlab_command,
    )
    prepare_test_file(
        "imap_mag_l1c_norm-mago-hundred-vectors_20250421_v001.cdf",
        "science/mag/l1c",
        2025,
        4,
        rename="imap_mag_l1c_norm-mago_20250421_v001.cdf",
    )

    calibrate(
        date=datetime(2025, 4, 21),
        sensor=Sensor.MAGO,
        mode=ScienceMode.Normal,
        method=CalibrationMethod.NOOP,
    )

    layer_metadata = (
        temp_datastore
        / "calibration/layers/2025/04/imap_mag_noop-layer-meta_20250421_v001.json"
    )
    assert layer_metadata.exists()
    with open(layer_metadata) as f:
        noop_layer = json.load(f)

    assert noop_layer["method"] == "noop"

    layer_data = (
        temp_datastore
        / "calibration/layers/2025/04/imap_mag_noop-layer-data_20250421_v001.csv"
    )
    assert layer_data.exists()
    with open(layer_data) as f:
        noop_data = pd.read_csv(layer_data)

    assert len(noop_data) == 100

    real_timestamps = [
        "2025-04-21T12:16:05.569359872",
        "2025-04-21T12:16:06.069359872",
        "2025-04-21T12:16:06.569359872",
        "2025-04-21T12:16:07.069359872",
    ]
    for val, timestamp in zip(noop_data.iterrows(), real_timestamps):
        assert np.datetime64(val[1]["time"]) == np.datetime64(timestamp)
        assert val[1]["offset_x"] == 0
        assert val[1]["offset_y"] == 0
        assert val[1]["offset_z"] == 0
        assert val[1]["timedelta"] is not None
        assert val[1]["quality_flag"] is not None
        assert val[1]["quality_bitmask"] is not None


@pytest.mark.skipif(
    not (os.getenv("MLM_LICENSE_FILE") or os.getenv("MLM_LICENSE_TOKEN"))
    or which(get_test_matlab_command()) is None,
    reason="MATLAB License not set or MATLAB is not available; skipping MATLAB tests",
)
def test_gradiometry_calibration_layer_is_created_with_correct_offsets_for_one_vector(
    matlab_test_setup,
    tmp_path,
    monkeypatch,
    temp_datastore,  # noqa: F811
):
    monkeypatch.setattr(
        "mag_toolkit.calibration.MatlabWrapper.get_matlab_command",
        get_test_matlab_command,
    )

    gradiometry(
        date=datetime(2026, 9, 30),
        mode=ScienceMode.Normal,
        kappa=0.25,
        sc_interference_threshold=10.0,
    )
    layer_metadata = (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-layer-meta_20260930_v001.json"
    )
    assert layer_metadata.exists()
    with open(layer_metadata) as f:
        grad_layer = json.load(f)

    assert grad_layer["method"] == "gradiometer"

    layer_data = (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-layer-data_20260930_v001.csv"
    )
    assert layer_data.exists()
    with open(layer_data) as f:
        grad_data = pd.read_csv(f)

    assert len(grad_data) == 99
    assert np.datetime64(grad_data["time"][0]) == np.datetime64(
        "2026-09-30T00:00:08.285840"
    ), (
        "First timestamp should match the MAGo first timestamp 2026-09-30T00:00:08.285840"
    )

    try:
        cal_layer = CalibrationLayer.from_file(layer_metadata)
    except Exception as e:
        pytest.fail(f"Calibration layer created did not conform to standards: {e}")

    assert cal_layer.values[1].value == [
        -20.948437287498678,
        287.80371538145209,
        350.14540002089052,
    ]
    assert cal_layer.values[1].quality_bitmask == 2
    assert cal_layer.metadata.comment == "Gradiometer layer with kappa value: 0.25"
