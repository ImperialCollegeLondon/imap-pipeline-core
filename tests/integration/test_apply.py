import re
import shutil
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest

from imap_mag.cli.apply import apply
from imap_mag.config import SaveMode
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationApplicator
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import TEST_DATA, copy_test_file, open_cdf


def verify_noop_results(datastore, date=datetime(2025, 10, 17), frame="srf"):
    output_l2_file = (
        datastore
        / f"science/mag/l2-pre/{date.year}/{date.month:02d}/imap_mag_l2-pre_norm-{frame}_{date.year}{date.month:02d}{date.day:02d}_v001.cdf"
    )
    assert output_l2_file.exists()
    output_offsets_file = (
        datastore
        / f"science-ancillary/l2-offsets/{date.year}/{date.month:02d}/imap_mag_l2-norm-offsets_{date.year}{date.month:02d}{date.day:02d}_{date.year}{date.month:02d}{date.day:02d}_v001.cdf"
    )
    assert output_offsets_file.exists()

    with open_cdf(output_offsets_file) as offsets_cdf:
        assert "offsets" in offsets_cdf
        assert "epoch" in offsets_cdf
        assert "timedeltas" in offsets_cdf
        assert "quality_flag" in offsets_cdf
        assert "quality_bitmask" in offsets_cdf

    with open_cdf(output_l2_file) as cdf:
        assert f"b_{frame}" in cdf
        assert "epoch" in cdf
        assert "magnitude" in cdf
        assert "quality_flags" in cdf
        assert "quality_bitmask" in cdf


def test_apply_produces_output_science_file_and_offsets_file_with_data(
    temp_datastore,
    capture_cli_logs,
    dynamic_work_folder,
    spice_kernels,
):
    apply(
        layers=["imap_mag_noop-norm-layer_20260116_v001.json"],
        input="imap_mag_l1c_norm-mago_20260116_v001.cdf",
        start_date=datetime(2026, 1, 16),
    )
    verify_noop_results(temp_datastore, date=datetime(2026, 1, 16))

    assert re.search(
        r"Calibration layer data defined in separate file: /.*/calibration/layers/2026/01/imap_mag_noop-norm-layer-data_20260116_v001.csv",
        capture_cli_logs.text,
    )


def test_apply_fails_when_timestamps_dont_align(temp_datastore, dynamic_work_folder):
    for f in [
        "imap_mag_misaligned-timestamps-norm-layer_20251017_v001.json",
        "imap_mag_misaligned-timestamps-norm-layer-data_20251017_v001.csv",
    ]:
        copy_test_file(
            TEST_DATA / f,
            temp_datastore / "calibration/layers/2025/10",
        )

    with pytest.raises(
        Exception, match="Offsets and science data are not time compatible"
    ):
        apply(
            layers=["imap_mag_misaligned-timestamps-norm-layer_20251017_v001.json"],
            input="imap_mag_l1c_norm-mago_20251017_v001.cdf",
            start_date=datetime(2025, 10, 17),
        )

    assert not (
        temp_datastore
        / "science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-srf_20251017_v001.cdf"
    ).exists()
    assert not (
        temp_datastore
        / "science-ancillary/l2-offsets/2025/10/imap_mag_l2-norm-offsets_20251017_20251017_v001.cdf"
    ).exists()


def test_apply_fails_when_no_layers_provided(temp_datastore, dynamic_work_folder):
    # No layers provided, should raise ValueError
    with pytest.raises(
        ValueError,
        match=re.escape(
            "At least one of calibration layers or rotation file must be provided"
        ),
    ):
        apply(
            layers=[],
            input="imap_mag_l1c_norm-mago_20251017_v001.cdf",
            start_date=datetime(2025, 10, 17),
        )

    assert not (
        temp_datastore
        / "science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-srf_20251017_v001.cdf"
    ).exists()
    assert not (
        temp_datastore
        / "science-ancillary/l2-offsets/2025/10/imap_mag_l2-norm-offsets_20251017_20251017_v001.cdf"
    ).exists()


def test_apply_errors_on_metadata_incorrect_data_filename_format(
    temp_datastore,
    dynamic_work_folder,
):
    # Set up.
    invalid_metadata_file = TEST_DATA / "metadata_file_no_metadata.json"
    calibration_layer = "imap_mag_noop-norm-layer_20251017_v001.json"

    copy_test_file(
        invalid_metadata_file,
        temp_datastore / "calibration/layers/2025/10",
        calibration_layer,
    )

    # Exercise and verify.
    with pytest.raises(
        Exception,
        match=re.escape("Field required"),
    ):
        apply(
            layers=[calibration_layer],
            input="imap_mag_l1c_norm-mago_20251017_v001.cdf",
            start_date=datetime(2025, 10, 17),
        )


def test_apply_performs_correct_rotation(
    dynamic_work_folder,
    temp_datastore,
    spice_kernels,
):
    copy_test_file(
        TEST_DATA / "imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        temp_datastore / "science/mag/l1c/2025/10",
        "imap_mag_l1c_norm-mago_20251017_v000.cdf",
    )

    apply(
        layers=[],
        input="imap_mag_l1c_norm-mago_20251017_v000.cdf",
        rotation=Path("imap_mag_l2-calibration_20250926_v002.cdf"),
        start_date=datetime(2025, 10, 17),
    )

    output_file = (
        temp_datastore
        / "science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-srf_20251017_v001.cdf"
    )

    assert output_file.exists()

    correct_vecs = [
        [23218.328, 9613.938, 36211.18],
        [23217.203, 9615.084, 36211.746],
        [23217.77, 9612.79, 36211.176],
        [23217.203, 9613.361, 36211.74],
    ]

    with open_cdf(output_file) as cdf:
        vectors = cdf["b_srf"][...]
        for correct_vec, vec in zip(correct_vecs, vectors):  # type: ignore
            # Convert to list for comparison
            assert vec == pytest.approx(correct_vec, rel=1e-6)


def test_apply_adds_offsets_together_correctly(
    dynamic_work_folder,
    temp_datastore,
    spice_kernels,
):
    for f in [
        "imap_mag_four-vector-offsets-norm-layer_20251017_v001.json",
        "imap_mag_four-vector-offsets-norm-layer-data_20251017_v001.csv",
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
        layers=["imap_mag_four-vector-offsets-norm-layer_20251017_v001.json"],
        input="imap_mag_l1c_norm-mago_20251017_v000.cdf",
        start_date=datetime(2025, 10, 17),
    )

    output_file = (
        temp_datastore
        / "science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-srf_20251017_v001.cdf"
    )

    assert output_file.exists()

    # Correct vectors calculated manually using MATLAB
    correct_vecs = [
        [23613.88873, 9783.16736, 35999.27727],
        [23604.93445, 9792.60397, 35985.69583],
        [23603.11448, 9785.58375, 36006.90274],
        [23584.60784, 9784.77406, 36042.23171],
    ]

    with open_cdf(output_file) as cdf:
        vectors = cdf["b_srf"][...]
        for correct_vec, vec in zip(correct_vecs, vectors):  # type: ignore
            # Convert to list for comparison
            assert vec == pytest.approx(correct_vec, rel=1e-6)


# TODO: Interpolation style calibration layers have bveen nuked with the move to pandas CSV files and dataframes but we will probably want to bring them back, in which case we will need this!
# def test_simple_interpolation_calibration_values_apply_correctly():
#     calibration_values = [
#         CalibrationValue(time=np.datetime64("2025-01-01T12:30"), value=[0, 0, 0]),
#         CalibrationValue(time=np.datetime64("2025-01-01T12:30:02"), value=[2, 0, 0]),
#     ]
#     science_values = [
#         ScienceValue(
#             time=np.datetime64("2025-01-01T12:30:01"), value=[0, 0, 0], range=3
#         )
#     ]

#     applier = CalibrationApplicator()

#     resulting_science, resulting_calibration = (
#         applier._apply_interpolation_points_to_science_values(
#             science_values, calibration_values
#         )
#     )

#     assert resulting_science[0].time == resulting_calibration[0].time
#     assert len(resulting_science) == len(resulting_calibration)
#     assert resulting_calibration[0].value == [1, 0, 0]
#     assert resulting_science[0].value == [1, 0, 0]


def test_apply_writes_magnitudes_correctly(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
):
    for f in [
        "imap_mag_four-vector-offsets-norm-layer_20251017_v001.json",
        "imap_mag_four-vector-offsets-norm-layer-data_20251017_v001.csv",
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
        layers=["imap_mag_four-vector-offsets-norm-layer_20251017_v001.json"],
        input="imap_mag_l1c_norm-mago-four-vectors-four-ranges_20251017_v000.cdf",
        start_date=datetime(2025, 10, 17),
    )

    output_file = (
        temp_datastore
        / "science/mag/l2-pre/2025/10/imap_mag_l2-pre_norm-srf_20251017_v001.cdf"
    )

    assert output_file.exists()

    # Correct vectors calculated manually using MATLAB
    correct_vecs = [
        [-9783.16736, -23613.88873, -35999.27727],
        [-9792.60397, -23604.93445, -35985.69583],
        [-9785.58375, -23603.11448, -36006.90274],
        [-9784.77406, -23584.60784, -36042.23171],
    ]

    with open_cdf(output_file) as cdf:
        magnitudes = cdf["magnitude"][...]
        for correct_vec, mag in zip(correct_vecs, magnitudes):  # type: ignore
            # Convert to list for comparison
            computed_magnitude = np.linalg.norm(correct_vec)
            assert mag == pytest.approx(computed_magnitude, rel=1e-6)


def test_apply_uses_absolute_path_for_science_file(
    tmp_path,
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
):
    """apply() must use a science file given as an absolute path that lives
    outside the datastore, rather than ignoring the path and looking in the
    datastore by filename.

    Bug: the old code extracted the filename, built a SciencePathHandler, then
    called datastore_finder.find_matching_file() which always searched the
    datastore.  An absolute path pointing to a copy of the file that was NOT
    in the datastore would raise FileNotFoundError even though the file existed.
    """
    # Copy the science file to a location outside the datastore
    science_in_datastore = (
        temp_datastore
        / "science/mag/l1c/2026/01/imap_mag_l1c_norm-mago_20260116_v001.cdf"
    )
    science_outside = tmp_path / "imap_mag_l1c_norm-mago_20260116_v001.cdf"
    shutil.copy(science_in_datastore, science_outside)

    # Remove the file from the datastore so the only copy is the external one
    science_in_datastore.unlink()

    # apply() must succeed using the absolute path directly
    apply(
        layers=["imap_mag_noop-norm-layer_20260116_v001.json"],
        input=str(science_outside),  # absolute path outside datastore
        start_date=datetime(2026, 1, 16),
    )
    verify_noop_results(temp_datastore, date=datetime(2026, 1, 16))


def test_apply_spice_metakernel_resolved_as_datastore_relative_path(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
):
    """apply() must resolve the spice_metakernel when given as a path relative
    to the datastore root (e.g. "spice/mk/metakernel.txt").

    Bug: CalibrationApplicator used Path(spice_metakernel) directly, which is
    relative to CWD.  A path like "spice/mk/metakernel.txt" would only work if
    the CWD happened to contain that structure; it failed if the file was under
    the datastore but not the CWD.
    """
    apply(
        layers=["imap_mag_noop-norm-layer_20260116_v001.json"],
        start_date=datetime(2026, 1, 16),
        mode=ScienceMode.Normal,
        save_mode=SaveMode.LocalOnly,
        # Pass the metakernel as a datastore-relative path; CWD does NOT have this path
        spice_metakernel=Path("spice/mk/metakernel.txt"),
    )
    verify_noop_results(temp_datastore, date=datetime(2026, 1, 16))


def test_apply_cleans_up_temp_files_from_work_folder(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
):
    """After apply() completes, temporary CDF and JSON files (science inputs,
    L2 outputs, offset files, and layer files) must be removed from the work
    folder.  Note: layer *data* CSV files are not currently tracked in
    files_to_cleanup and are therefore not tested here."""
    apply(
        layers=["imap_mag_noop-norm-layer_20260116_v001.json"],
        input="imap_mag_l1c_norm-mago_20260116_v001.cdf",
        start_date=datetime(2026, 1, 16),
    )

    remaining_cdf = list(dynamic_work_folder.rglob("*.cdf"))
    remaining_json = list(dynamic_work_folder.rglob("*.json"))
    assert remaining_cdf == [], (
        f"Expected no CDF files in work folder, found: {remaining_cdf}"
    )
    assert remaining_json == [], (
        f"Expected no JSON files in work folder, found: {remaining_json}"
    )


def test_apply_does_not_delete_files_outside_work_folder(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
    capture_cli_logs,
):
    """Files resolved outside the work folder must not be deleted, and a warning
    must be logged."""
    # Create a sentinel file outside the work folder to stand in for an
    # external L2 output file returned by the applicator.
    external_file = temp_datastore / "should_not_be_deleted.cdf"
    external_file.touch()

    original_apply = CalibrationApplicator.apply

    def _patched_apply(self, *args, **kwargs):
        l2_files, offset_file = original_apply(self, *args, **kwargs)
        # Inject the external file so the cleanup loop encounters a path
        # that lives outside the work folder.
        return [*l2_files, external_file], offset_file

    with patch.object(CalibrationApplicator, "apply", _patched_apply):
        apply(
            layers=["imap_mag_noop-norm-layer_20260116_v001.json"],
            input="imap_mag_l1c_norm-mago_20260116_v001.cdf",
            start_date=datetime(2026, 1, 16),
        )

    assert external_file.exists(), "File outside work folder must not be deleted"
    assert re.search(
        r"Skipping deletion of file outside work folder.*should_not_be_deleted\.cdf",
        capture_cli_logs.text,
    )
