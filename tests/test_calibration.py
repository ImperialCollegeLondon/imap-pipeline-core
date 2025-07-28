import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from shutil import which

import numpy as np
import pytest
from spacepy import pycdf

from imap_mag.cli.apply import apply
from imap_mag.cli.calibrate import calibrate
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationMethod, Sensor
from mag_toolkit.calibration.MatlabWrapper import setup_matlab_path

from .util.miscellaneous import (  # noqa: F401
    create_test_file,
    tidyDataFolders,
)


def prepare_test_file(test_file, sub_folders, year=None, month=None, rename=None):
    """
    Prepare a calibration test file by copying the specified calibration layer
    to the output directory with the correct folder structure.
    """
    dest_filename = test_file if rename is None else rename
    if year and month:
        dest_filepath = (
            Path("output") / sub_folders / str(year) / f"{month:02d}" / dest_filename
        )
        original_filepath = f"tests/data/{sub_folders}/{year}/{month:02d}/{test_file}"
    else:
        dest_filepath = Path("output") / sub_folders / dest_filename
        original_filepath = Path("tests/data/") / sub_folders / test_file
    os.makedirs(dest_filepath.parent, exist_ok=True)
    shutil.copy(
        original_filepath,
        dest_filepath,
    )


def test_apply_produces_output_science_file_and_offsets_file_with_data(tmp_path):
    prepare_test_file(
        "imap_mag_l1c_norm-mago_20251017_v001.cdf",
        "science/mag/l1c",
        2025,
        10,
    )

    prepare_test_file(
        "imap_mag_noop-layer_20251017_v001.json",
        "calibration/layers",
        2025,
        10,
    )

    apply(
        layers=["imap_mag_noop-layer_20251017_v001.json"],
        input="imap_mag_l1c_norm-mago_20251017_v001.cdf",
        date=datetime(2025, 10, 17),
    )
    output_l2_file = (
        "output/science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-mago_20251017_v000.cdf"
    )
    assert Path(output_l2_file).exists()
    output_offsets_file = "output/science-ancillary/l2-offsets/2025/10/imap_mag_l2-norm-offsets_20251017_20251017_v000.cdf"
    assert Path(output_offsets_file).exists()

    with pycdf.CDF(output_offsets_file) as offsets_cdf:
        assert "offsets" in offsets_cdf
        assert "epoch" in offsets_cdf
        assert "timedeltas" in offsets_cdf
        assert "quality_flag" in offsets_cdf
        assert "quality_bitmask" in offsets_cdf

    with pycdf.CDF(output_l2_file) as cdf:
        assert "vectors" in cdf
        assert "epoch" in cdf
        assert "magnitude" in cdf
        assert "quality_flags" in cdf
        assert "quality_bitmask" in cdf


def test_apply_fails_when_timestamps_dont_align(tmp_path):
    prepare_test_file(
        "imap_mag_l1c_norm-mago_20251017_v001.cdf",
        "science/mag/l1c",
        2025,
        10,
    )
    prepare_test_file(
        "imap_mag_misaligned-timestamps-layer_20251017_v001.json",
        "calibration/layers",
        2025,
        10,
    )
    with pytest.raises(Exception) as exc_info:
        apply(
            layers=["imap_mag_misaligned-timestamps-layer_20251017_v001.json"],
            input="imap_mag_l1c_norm-mago_20251017_v001.cdf",
            date=datetime(2025, 10, 17),
        )

    assert not Path(
        "output/science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-mago_20251017_v001.cdf"
    ).exists()
    assert not Path(
        "output/science-ancillary/l2-offsets/2025/10/imap_mag_l2-norm-offsets_20251017_20251017_v001.cdf"
    ).exists()

    assert str(exc_info.value) == "Layer and data timestamps do not align"


def test_apply_fails_when_no_layers_provided(tmp_path):
    prepare_test_file(
        "imap_mag_l1c_norm-mago_20251017_v001.cdf",
        "science/mag/l1c",
        2025,
        10,
    )
    # No layers provided, should raise ValueError
    with pytest.raises(ValueError) as exc_info:
        apply(
            layers=[],
            input="imap_mag_l1c_norm-mago_20251017_v001.cdf",
            date=datetime(2025, 10, 17),
        )

    assert not Path(
        "output/science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-mago_20251017_v000.cdf"
    ).exists()
    assert not Path(
        "output/science-ancillary/l2-offsets/2025/10/imap_mag_l2_norm-offsets_20251017_20251017_v000.cdf"
    ).exists()

    assert str(exc_info.value) == "No calibration layers or rotation file provided"


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


def test_apply_adds_offsets_together_correctly(tmp_path):
    prepare_test_file(
        "imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        "science/mag/l1c",
        2025,
        10,
        rename="imap_mag_l1c_norm-mago_20251017_v000.cdf",
    )
    prepare_test_file(
        "imap_mag_four-vector-offsets-layer_20251017_v001.json",
        "calibration/layers",
        2025,
        10,
    )
    apply(
        layers=["imap_mag_four-vector-offsets-layer_20251017_v001.json"],
        input="imap_mag_l1c_norm-mago_20251017_v000.cdf",
        date=datetime(2025, 10, 17),
    )

    output_file = (
        "output/science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-mago_20251017_v000.cdf"
    )

    assert Path(output_file).exists()

    # Correct vectors calculated manually using MATLAB
    correct_vecs = [
        [-9783.16736, -23613.88873, -35999.27727],
        [-9792.60397, -23604.93445, -35985.69583],
        [-9785.58375, -23603.11448, -36006.90274],
        [-9784.77406, -23584.60784, -36042.23171],
    ]

    with pycdf.CDF(output_file) as cdf:
        vectors = cdf["vectors"][...]
        for correct_vec, vec in zip(correct_vecs, vectors):  # type: ignore
            # Convert to list for comparison
            assert vec == pytest.approx(correct_vec, rel=1e-6)


def test_apply_writes_magnitudes_correctly(tmp_path):
    prepare_test_file(
        "imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        "science/mag/l1c",
        2025,
        10,
    )
    prepare_test_file(
        "imap_mag_four-vector-offsets-layer_20251017_v001.json",
        "calibration/layers",
        2025,
        10,
    )
    apply(
        layers=["imap_mag_four-vector-offsets-layer_20251017_v001.json"],
        input="imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        date=datetime(2025, 10, 17),
    )

    output_file = "output/science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-mago-four-vectors-four-ranges_20251017_v000.cdf"

    assert Path(output_file).exists()

    # Correct vectors calculated manually using MATLAB
    correct_vecs = [
        [-9783.16736, -23613.88873, -35999.27727],
        [-9792.60397, -23604.93445, -35985.69583],
        [-9785.58375, -23603.11448, -36006.90274],
        [-9784.77406, -23584.60784, -36042.23171],
    ]

    with pycdf.CDF(output_file) as cdf:
        magnitudes = cdf["magnitude"][...]
        for correct_vec, mag in zip(correct_vecs, magnitudes):  # type: ignore
            # Convert to list for comparison
            computed_magnitude = np.linalg.norm(correct_vec)
            assert mag == pytest.approx(computed_magnitude, rel=1e-6)


@pytest.fixture()
def matlab_test_setup():
    # Code that will run before your test, for example:
    setup_matlab_path("src/matlab", "matlab")
    # A test function will be run at this point
    yield


@pytest.mark.skipif(
    not (os.getenv("MLM_LICENSE_FILE") or os.getenv("MLM_LICENSE_TOKEN"))
    or which("matlab") is None,
    reason="MATLAB License not set or MATLAB is not available; skipping MATLAB tests",
)
def test_calibration_layer_is_created_with_offsets_for_every_vector(
    matlab_test_setup, tmp_path
):
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
    assert Path(
        "output/calibration/layers/2025/04/imap_mag_noop-layer_20250421_v001.json"
    ).exists()
    with open(
        "output/calibration/layers/2025/04/imap_mag_noop-layer_20250421_v001.json"
    ) as f:
        noop_layer = json.load(f)

    assert noop_layer["method"] == "noop"
    assert len(noop_layer["values"]) == 100

    real_timestamps = [
        "2025-04-21T12:16:05.569359872",
        "2025-04-21T12:16:06.069359872",
        "2025-04-21T12:16:06.569359872",
        "2025-04-21T12:16:07.069359872",
    ]
    for val, timestamp in zip(noop_layer["values"], real_timestamps):
        assert np.datetime64(val["time"]) == np.datetime64(timestamp)
        offset = val["value"]
        assert len(offset) == 3
        assert offset[0] == 0
        assert offset[1] == 0
        assert offset[2] == 0
        assert val["timedelta"] is not None
        assert val["quality_flag"] is not None
        assert val["quality_bitmask"] is not None
