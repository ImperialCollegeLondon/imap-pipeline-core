import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pytest
from spacepy import pycdf

from imap_mag.cli.apply import CalibrationApplicator, apply
from mag_toolkit.calibration import (
    CalibrationValue,
    ScienceValue,
)
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import TEST_DATA, copy_test_file


def verify_noop_20251017_results(datastore):
    output_l2_file = (
        datastore
        / "science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-mago_20251017_v000.cdf"
    )
    assert output_l2_file.exists()
    output_offsets_file = (
        datastore
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


def test_apply_produces_output_science_file_and_offsets_file_with_data(
    temp_datastore,
    capture_cli_logs,
    dynamic_work_folder,
    test_database,  # noqa: F811
):
    apply(
        layers=["imap_mag_noop-layer_20251017_v001.json"],
        input="imap_mag_l1c_norm-mago_20251017_v001.cdf",
        date=datetime(2025, 10, 17),
    )
    verify_noop_20251017_results(temp_datastore)

    assert re.search(
        r"Calibration layer data defined in separate file: /.*/calibration/layers/2025/10/imap_mag_noop-layer-data_20251017_v001.csv",
        capture_cli_logs.text,
    )


def test_apply_works_with_metadata_files_containing_values(
    temp_datastore,
    capture_cli_logs,
    dynamic_work_folder,
    test_database,  # noqa: F811
):
    # Set up.
    calibration_layer = "imap_mag_noop-layer_20251017_v002.json"

    copy_test_file(
        TEST_DATA / "metadata_file_with_values.json",
        temp_datastore / "calibration/layers/2025/10",
        calibration_layer,
    )

    # Exercise.
    apply(
        layers=[calibration_layer],
        input="imap_mag_l1c_norm-mago_20251017_v001.cdf",
        date=datetime(2025, 10, 17),
    )

    # Verify.
    verify_noop_20251017_results(temp_datastore)

    assert (
        "Calibration layer data defined in metadata file. No separate file will be used."
        in capture_cli_logs.text
    )


def test_apply_fails_when_timestamps_dont_align(temp_datastore, dynamic_work_folder):
    for f in [
        "imap_mag_misaligned-timestamps-layer_20251017_v001.json",
        "imap_mag_misaligned-timestamps-layer-data_20251017_v001.csv",
    ]:
        copy_test_file(
            TEST_DATA / f,
            temp_datastore / "calibration/layers/2025/10",
        )

    with pytest.raises(Exception, match="Layer and data timestamps do not align"):
        apply(
            layers=["imap_mag_misaligned-timestamps-layer_20251017_v001.json"],
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


def test_apply_fails_when_no_layers_provided(temp_datastore, dynamic_work_folder):
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


def test_apply_errors_on_metadata_incorrect_data_filename_format(
    temp_datastore,
    dynamic_work_folder,
    test_database,  # noqa: F811
):
    # Set up.
    invalid_metadata_file = TEST_DATA / "metadata_file_no_metadata.json"
    calibration_layer = "imap_mag_noop-layer_20251017_v001.json"

    copy_test_file(
        invalid_metadata_file,
        temp_datastore / "calibration/layers/2025/10",
        calibration_layer,
    )

    # Exercise and verify.
    with pytest.raises(
        Exception,
        match="Field required",
    ):
        apply(
            layers=[calibration_layer],
            input="imap_mag_l1c_norm-mago_20251017_v001.cdf",
            date=datetime(2025, 10, 17),
        )


def test_apply_performs_correct_rotation(
    dynamic_work_folder,
    temp_datastore,
    test_database,  # noqa: F811
):
    copy_test_file(
        TEST_DATA / "imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        temp_datastore / "science/mag/l1c/2025/10",
        "imap_mag_l1c_norm-mago_20251017_v000.cdf",
    )

    apply(
        layers=[],
        input="imap_mag_l1c_norm-mago_20251017_v000.cdf",
        rotation=Path("imap_mag_l2-calibration_20251017_v004.cdf"),
        date=datetime(2025, 10, 17),
    )

    output_file = (
        temp_datastore
        / "science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-mago_20251017_v000.cdf"
    )

    assert output_file.exists()

    # Correct vectors calculated manually using MATLAB
    correct_vecs = [
        [-7351.97046454686, -25221.0251719171, -36006.8993],
        [-8847.56935760261, -25370.1800953166, -36000.26330704],
        [-9476.9412947865, -23426.8605232176, -36124.2844555925],
        [-9654.64764180912, -27472.6841916782, -35709.9103366105],
    ]

    with pycdf.CDF(str(output_file)) as cdf:
        vectors = cdf["vectors"][...]
        for correct_vec, vec in zip(correct_vecs, vectors):  # type: ignore
            # Convert to list for comparison
            assert vec == pytest.approx(correct_vec, rel=1e-6)


def test_apply_adds_offsets_together_correctly(
    dynamic_work_folder,
    temp_datastore,
    test_database,  # noqa: F811
):
    for f in [
        "imap_mag_four-vector-offsets-layer_20251017_v001.json",
        "imap_mag_four-vector-offsets-layer-data_20251017_v001.csv",
    ]:
        copy_test_file(
            TEST_DATA / f,
            temp_datastore / "calibration/layers/2025/10",
        )

    copy_test_file(
        TEST_DATA / "imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        temp_datastore / "science/mag/l1c/2025/10",
        "imap_mag_l1c_norm-mago_20251017_v000.cdf",
    )

    apply(
        layers=["imap_mag_four-vector-offsets-layer_20251017_v001.json"],
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


def test_apply_writes_magnitudes_correctly(
    temp_datastore,
    dynamic_work_folder,
    test_database,  # noqa: F811
):
    for f in [
        "imap_mag_four-vector-offsets-layer_20251017_v001.json",
        "imap_mag_four-vector-offsets-layer-data_20251017_v001.csv",
    ]:
        copy_test_file(
            TEST_DATA / f,
            temp_datastore / "calibration/layers/2025/10",
        )

    copy_test_file(
        TEST_DATA / "imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        temp_datastore / "science/mag/l1c/2025/10",
    )

    apply(
        layers=["imap_mag_four-vector-offsets-layer_20251017_v001.json"],
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
