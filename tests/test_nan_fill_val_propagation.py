"""Test that CDF Fill Values (NaN indicators) propagate correctly through
calibration and apply steps.

The rule: whenever you apply a layer, a NaN should always propagate — NaNs
override all offsets in layers.

This test verifies two NaN propagation paths:
1. A NaN/Fill Value in the source science CDF file propagates through to offsets
   and the final L2-pre CDF.
2. A NaN/Fill Value introduced in a calibration layer CSV also propagates
   through to offsets and the final L2-pre CDF.
"""

import glob
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from imap_mag.cli.apply import apply
from imap_mag.util.ReferenceFrame import ReferenceFrame
from mag_toolkit.calibration import CalibrationLayer, ScienceLayer
from mag_toolkit.calibration.CalibrationDefinitions import CONSTANTS
from tests.util.miscellaneous import open_cdf

FILL = CONSTANTS.CDF_FLOAT_FILLVAL
DATE = datetime(2026, 1, 16)
SCIENCE_FILE = "imap_mag_l1c_norm-mago_20260116_v001.cdf"

# Row indices we'll inject fill values into
SCIENCE_FILL_ROW = 3  # row in science CDF set to fill value
LAYER_FILL_ROW = 7  # different row in layer CSV set to fill value


def _set_science_cdf_fill_value(path: Path, row_index: int):
    """Overwrite one row's vectors with CDF Fill Value in a science CDF."""
    with open_cdf(path, readonly=False) as cdf:
        vectors = cdf["vectors"][...]
        vectors[row_index, 0] = FILL  # x
        vectors[row_index, 1] = FILL  # y
        vectors[row_index, 2] = FILL  # z
        cdf["vectors"] = vectors


def _create_noop_layer(science_cdf_path: Path, output_folder: Path):
    """Create a noop (zero-offset) calibration layer from a science CDF file.

    Returns the path to the layer JSON metadata file.
    """
    science_layer = ScienceLayer.from_file(science_cdf_path, load_contents=True)
    zero_layer = CalibrationLayer.create_zero_offset_layer_from_science(science_layer)

    layer_json = output_folder / "imap_mag_noop-norm-layer_20260116_v001.json"
    zero_layer.writeToFile(layer_json)
    return layer_json


def _find_latest_file(pattern: str) -> Path:
    """Find the latest file matching a glob pattern."""
    matches = sorted(glob.glob(pattern))
    assert matches, f"No files found matching {pattern}"
    return Path(matches[-1])


def test_nan_fill_values_propagate_through_calibration_and_apply(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
):
    """Verify that CDF Fill Values propagate from science files and layer CSVs
    through the apply step into both the offsets CDF and L2-pre CDF files."""

    # Remove pre-existing offsets files to avoid version conflicts
    existing_offsets = temp_datastore / "science-ancillary/l2-offsets"
    if existing_offsets.exists():
        import shutil

        shutil.rmtree(existing_offsets)

    modified_science = temp_datastore / f"science/mag/l1c/2026/01/{SCIENCE_FILE}"

    # ---- Step 1: Modify science CDF — set one row to CDF Fill Value ----
    _set_science_cdf_fill_value(modified_science, SCIENCE_FILL_ROW)

    # Verify the modification took effect
    with open_cdf(modified_science) as cdf:
        vectors = cdf["vectors"][...]
        assert vectors[SCIENCE_FILL_ROW, 0] == FILL
        assert vectors[SCIENCE_FILL_ROW, 1] == FILL
        assert vectors[SCIENCE_FILL_ROW, 2] == FILL

    # ---- Step 2: Create noop layer from the modified science file ----
    layer_json = _create_noop_layer(
        modified_science,
        temp_datastore / "calibration/layers/2026/01",
    )
    assert layer_json.exists()

    # Verify noop layer CSV — all offsets should be 0, including the fill-value row
    layer_csv = layer_json.parent / "imap_mag_noop-layer-data_20260116_v001.csv"
    assert layer_csv.exists()

    layer_df = pd.read_csv(layer_csv)
    assert len(layer_df) == 16  # same number of rows as science file

    # The fill-value row should still have zero offsets (noop doesn't alter offsets)
    assert layer_df.iloc[SCIENCE_FILL_ROW]["offset_x"] == 0.0
    assert layer_df.iloc[SCIENCE_FILL_ROW]["offset_y"] == 0.0
    assert layer_df.iloc[SCIENCE_FILL_ROW]["offset_z"] == 0.0

    # ---- Step 3: Edit the layer CSV to set a DIFFERENT row to CDF Fill Value ----
    layer_df.at[LAYER_FILL_ROW, "offset_x"] = FILL
    layer_df.at[LAYER_FILL_ROW, "offset_y"] = FILL
    layer_df.at[LAYER_FILL_ROW, "offset_z"] = FILL
    layer_df.to_csv(layer_csv, index=False)

    # Verify the edit
    reloaded = pd.read_csv(layer_csv)
    assert reloaded.iloc[LAYER_FILL_ROW]["offset_x"] == FILL
    assert reloaded.iloc[LAYER_FILL_ROW]["offset_y"] == FILL
    assert reloaded.iloc[LAYER_FILL_ROW]["offset_z"] == FILL

    # ---- Step 4: Apply the layer to generate offsets and L2-pre files ----
    apply(
        layers=["imap_mag_noop-norm-layer_20260116_v001.json"],
        input=SCIENCE_FILE,
        start_date=DATE,
        reference_frames=[ReferenceFrame.SRF],
    )

    # ---- Step 5: Verify offsets CDF ----
    offsets_file = _find_latest_file(
        str(
            temp_datastore
            / "science-ancillary/l2-offsets/2026/01/imap_mag_l2-norm-offsets_20260116_20260116_v*.cdf"
        )
    )

    with open_cdf(offsets_file) as offsets_cdf:
        offsets = offsets_cdf["offsets"][...]

        # Row from LAYER fill value — the layer explicitly set fill values
        assert offsets[LAYER_FILL_ROW, 0] == pytest.approx(FILL, rel=1e-6), (
            f"Layer fill-val row offset_x should be {FILL}, got {offsets[LAYER_FILL_ROW, 0]}"
        )
        assert offsets[LAYER_FILL_ROW, 1] == pytest.approx(FILL, rel=1e-6), (
            f"Layer fill-val row offset_y should be {FILL}, got {offsets[LAYER_FILL_ROW, 1]}"
        )
        assert offsets[LAYER_FILL_ROW, 2] == pytest.approx(FILL, rel=1e-6), (
            f"Layer fill-val row offset_z should be {FILL}, got {offsets[LAYER_FILL_ROW, 2]}"
        )

        # Row from SCIENCE fill value — the science data was fill val, so the
        # offset should also be marked as fill value (NaN propagation)
        assert offsets[SCIENCE_FILL_ROW, 0] == pytest.approx(FILL, rel=1e-6), (
            f"Science fill-val row offset_x should be {FILL}, got {offsets[SCIENCE_FILL_ROW, 0]}"
        )
        assert offsets[SCIENCE_FILL_ROW, 1] == pytest.approx(FILL, rel=1e-6), (
            f"Science fill-val row offset_y should be {FILL}, got {offsets[SCIENCE_FILL_ROW, 1]}"
        )
        assert offsets[SCIENCE_FILL_ROW, 2] == pytest.approx(FILL, rel=1e-6), (
            f"Science fill-val row offset_z should be {FILL}, got {offsets[SCIENCE_FILL_ROW, 2]}"
        )

        # All other rows should be zero (noop offsets)
        for i in range(len(offsets)):
            if i in (SCIENCE_FILL_ROW, LAYER_FILL_ROW):
                continue
            assert offsets[i, 0] == 0.0, (
                f"Row {i} offset_x should be 0, got {offsets[i, 0]}"
            )
            assert offsets[i, 1] == 0.0, (
                f"Row {i} offset_y should be 0, got {offsets[i, 1]}"
            )
            assert offsets[i, 2] == 0.0, (
                f"Row {i} offset_z should be 0, got {offsets[i, 2]}"
            )

    # ---- Step 6: Verify L2-pre CDF ----
    l2pre_file = _find_latest_file(
        str(
            temp_datastore
            / "science/mag/l2-pre/2026/01/imap_mag_l2-pre_norm-srf_20260116_v*.cdf"
        )
    )

    with open_cdf(l2pre_file) as l2_cdf:
        b_srf = l2_cdf["b_srf"][...]

        # Row from SCIENCE fill value — should have fill values in L2-pre
        assert b_srf[SCIENCE_FILL_ROW, 0] == pytest.approx(FILL, rel=1e-6), (
            f"L2-pre science fill-val row x should be {FILL}, got {b_srf[SCIENCE_FILL_ROW, 0]}"
        )
        assert b_srf[SCIENCE_FILL_ROW, 1] == pytest.approx(FILL, rel=1e-6), (
            f"L2-pre science fill-val row y should be {FILL}, got {b_srf[SCIENCE_FILL_ROW, 1]}"
        )
        assert b_srf[SCIENCE_FILL_ROW, 2] == pytest.approx(FILL, rel=1e-6), (
            f"L2-pre science fill-val row z should be {FILL}, got {b_srf[SCIENCE_FILL_ROW, 2]}"
        )

        # Row from LAYER fill value — should have fill values in L2-pre
        assert b_srf[LAYER_FILL_ROW, 0] == pytest.approx(FILL, rel=1e-6), (
            f"L2-pre layer fill-val row x should be {FILL}, got {b_srf[LAYER_FILL_ROW, 0]}"
        )
        assert b_srf[LAYER_FILL_ROW, 1] == pytest.approx(FILL, rel=1e-6), (
            f"L2-pre layer fill-val row y should be {FILL}, got {b_srf[LAYER_FILL_ROW, 1]}"
        )
        assert b_srf[LAYER_FILL_ROW, 2] == pytest.approx(FILL, rel=1e-6), (
            f"L2-pre layer fill-val row z should be {FILL}, got {b_srf[LAYER_FILL_ROW, 2]}"
        )
