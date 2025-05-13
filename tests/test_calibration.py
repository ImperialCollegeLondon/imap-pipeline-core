import json
import os
from datetime import datetime
from pathlib import Path
from shutil import which

import pytest
from spacepy import pycdf

from imap_mag.api.apply import apply
from imap_mag.api.calibrate import calibrate
from mag_toolkit.calibration import CalibrationMethod
from mag_toolkit.calibration.MatlabWrapper import setup_matlab_path

from .testUtils import create_test_file, enableLogging, tidyDataFolders  # noqa: F401


def test_apply_produces_output_science_file_and_offsets_file(tmp_path):
    test_config_content = """
    source:
        folder: tests/data/imap/mag/l1c/2025/10
        calibration_folder: tests/data/imap/mag/calibration/2025/10

    work-folder: .work

    destination:
        folder: output
        filename: imap_mag_l2_norm-mago_20251017_v001.cdf
    """

    test_config_path = create_test_file(
        tmp_path / "test_calibration_application_configuration.yml",
        test_config_content,
    )
    apply(
        layers=["20251017_20251017_noop_v001.json"],
        input="imap_mag_l1c_norm-mago_20251017_v001.cdf",
        config=test_config_path,
        from_date=datetime(2025, 10, 17),
        to_date=datetime(2025, 10, 17),
    )

    assert Path("output/2025/10/imap_mag_l2_norm-mago_20251017_v001.cdf").exists()
    assert Path("output/2025/10/imap_mag_l2_norm-offsets_20251017_v001.cdf").exists()


def test_apply_fails_when_timestamps_dont_align(tmp_path):
    test_config_content = """
    source:
        folder: tests/data/imap/mag/l1c/2025/10
        calibration_folder: tests/data/imap/mag/calibration/2025/10

    work-folder: .work

    destination:
        folder: output
        filename: imap_mag_l2_norm-mago_20251017_v001.cdf
    """

    test_config_path = create_test_file(
        tmp_path / "test_calibration_application_configuration.yml",
        test_config_content,
    )
    with pytest.raises(Exception) as exc_info:
        apply(
            layers=["misaligned_timestamps_calibration_layer.json"],
            input="imap_mag_l1c_norm-mago_20251017_v001.cdf",
            config=test_config_path,
            from_date=datetime(2025, 10, 17),
            to_date=datetime(2025, 10, 17),
        )

    assert not Path(
        "output/l2/2025/10/imap_mag_l2_norm-mago_20251017_v001.cdf"
    ).exists()
    assert not Path(
        "output/l2/2025/10/imap_mag_l2_norm-offsets_20251017_v001.cdf"
    ).exists()

    assert str(exc_info.value) == "Layer and data timestamps do not align"


def test_apply_fails_when_no_layers_provided(tmp_path):
    test_config_content = """
    source:
        folder: tests/data/imap/mag/l1c/2025/10
        calibration_folder: tests/data/imap/mag/calibration/2025/10

    work-folder: .work

    destination:
        folder: output
        filename: imap_mag_l2_norm-mago_20251017_v001.cdf
    """

    test_config_path = create_test_file(
        tmp_path / "test_calibration_application_configuration.yml",
        test_config_content,
    )
    with pytest.raises(ValueError) as exc_info:
        apply(
            layers=[],
            input="imap_mag_l1c_norm-mago_20251017_v001.cdf",
            config=test_config_path,
            from_date=datetime(2025, 10, 17),
            to_date=datetime(2025, 10, 17),
        )

    assert not Path(
        "output/l2/2025/10/imap_mag_l2_norm-mago_20251017_v001.cdf"
    ).exists()
    assert not Path(
        "output/l2/2025/10/imap_mag_l2_norm-offsets_20251017_v001.cdf"
    ).exists()

    assert str(exc_info.value) == "No calibration layers or rotation file provided"


def test_apply_performs_correct_rotation(tmp_path):
    test_config_content = """
    source:
        folder: tests/data/imap/mag/l1c/2025/10
        calibration_folder: tests/data/imap/mag/calibration/2025/10

    work-folder: .work

    destination:
        folder: output
        filename: imap_mag_l2_norm-mago_20251017_v001.cdf
    """

    test_config_path = create_test_file(
        tmp_path / "test_calibration_application_configuration.yml",
        test_config_content,
    )
    apply(
        layers=[],
        input="imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        rotation=Path(
            "tests/data/imap/mag/calibration/2025/10/imap_mag_l2-calibration-matrices_20251017_v004.cdf"
        ),
        config=test_config_path,
        from_date=datetime(2025, 10, 17),
        to_date=datetime(2025, 10, 17),
    )

    output_file = "output/2025/10/imap_mag_l2_norm-mago_20251017_v001.cdf"

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
    test_config_content = """
    source:
        folder: tests/data/imap/mag/l1c/2025/10
        calibration_folder: tests/data/imap/mag/calibration/2025/10

    work-folder: .work

    destination:
        folder: output
        filename: imap_mag_l2_norm-mago_20251017_v001.cdf
    """

    test_config_path = create_test_file(
        tmp_path / "test_calibration_application_configuration.yml",
        test_config_content,
    )
    apply(
        layers=["four_vector_offsets_cal_layer.json"],
        input="imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        config=test_config_path,
        from_date=datetime(2025, 10, 17),
        to_date=datetime(2025, 10, 17),
    )

    output_file = "output/2025/10/imap_mag_l2_norm-mago_20251017_v001.cdf"

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
    test_config_content = """
    source:
        folder: tests/data/imap/mag/l1c/2025/10
        calibration_folder: tests/data/imap/mag/calibration/2025/10

    work-folder: .work

    destination:
        folder: output
        filename: imap_mag_l2_norm-mago_20251017_v001.json
    """

    test_config_path = create_test_file(
        tmp_path / "test_calibration_configuration.yml",
        test_config_content,
    )

    calibrate(
        from_date=datetime(2025, 10, 17),
        to_date=datetime(2025, 10, 17),
        method=CalibrationMethod.NOOP,
        config=test_config_path,
        input="imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
    )
    assert Path("output/2025/10/20251017_20251017_noop_v000.json").exists()
    with open("output/2025/10/20251017_20251017_noop_v000.json") as f:
        noop_layer = json.load(f)

    assert noop_layer["method"] == "noop"
    assert len(noop_layer["values"]) == 4

    format = "%Y-%m-%dT%H:%M:%S.%f"
    real_timestamps = [
        "2025-10-17T02:11:51.521309",
        "2025-10-17T02:11:52.021309",
        "2025-10-17T02:11:52.521309",
        "2025-10-17T02:11:53.021309",
    ]
    for val, timestamp in zip(noop_layer["values"], real_timestamps):
        assert datetime.strptime(val["time"], format) == datetime.strptime(
            timestamp, format
        )
        offset = val["value"]
        assert len(offset) == 3
        assert offset[0] == 0
        assert offset[1] == 0
        assert offset[2] == 0
