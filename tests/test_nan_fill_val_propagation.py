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


def _create_noop_layer(science_cdf_path: Path, output_folder: Path, name: str = "noop"):
    science_layer = ScienceLayer.from_file(science_cdf_path, load_contents=True)
    zero_layer = CalibrationLayer.create_zero_offset_layer_from_science(science_layer)

    layer_json = output_folder / f"imap_mag_{name}-norm-layer_20260116_v001.json"
    zero_layer.writeToFile(layer_json)
    return layer_json


def _find_latest_file(pattern: str) -> Path:
    """Find the latest file matching a glob pattern."""
    matches = sorted(glob.glob(pattern))
    assert matches, f"No files found matching {pattern}"
    return Path(matches[-1])


def test_nan_fill_values_propagate_through_calibration_and_apply_from_layers_and_from_science(
    temp_datastore,
    dynamic_work_folder,
    spice_kernels,
):
    # setup science with a FILL val
    modified_science = temp_datastore / f"science/mag/l1c/2026/01/{SCIENCE_FILE}"
    _set_science_cdf_fill_value(modified_science, SCIENCE_FILL_ROW)

    base_layer_json = _create_noop_layer(
        modified_science,
        temp_datastore / "calibration/layers/2026/01",
        name="base-noop",
    )
    layer_json = _create_noop_layer(
        modified_science, temp_datastore / "calibration/layers/2026/01"
    )
    layer_csv = layer_json.parent / "imap_mag_noop-layer-data_20260116_v001.csv"
    assert layer_json.exists()
    assert layer_csv.exists()

    # Setup layer with a different time as NaN
    layer_df = pd.read_csv(layer_csv)
    layer_df.at[LAYER_FILL_ROW, "offset_x"] = float("nan")
    layer_df.at[LAYER_FILL_ROW, "offset_y"] = float("nan")
    layer_df.at[LAYER_FILL_ROW, "offset_z"] = float("nan")
    layer_df.to_csv(layer_csv, index=False)

    apply(
        layers=[
            base_layer_json.name,
            layer_json.name,
        ],
        input=SCIENCE_FILE,
        start_date=DATE,
        reference_frames=[ReferenceFrame.SRF],
    )

    offsets_file = _find_latest_file(
        str(
            temp_datastore
            / "science-ancillary/l2-offsets/2026/01/imap_mag_l2-norm-offsets_20260116_20260116_v*.cdf"
        )
    )

    # Verify offsets file
    with open_cdf(offsets_file) as offsets_cdf:
        offsets = offsets_cdf["offsets"][...]

        # Row from LAYER fill value — the layer explicitly set fill values
        assert offsets[LAYER_FILL_ROW, 0] == FILL
        assert offsets[LAYER_FILL_ROW, 1] == FILL
        assert offsets[LAYER_FILL_ROW, 2] == FILL

        # Row from SCIENCE fill value — the science data was fill val but offset file can still be zero
        assert offsets[SCIENCE_FILL_ROW, 0] == 0
        assert offsets[SCIENCE_FILL_ROW, 1] == 0
        assert offsets[SCIENCE_FILL_ROW, 2] == 0

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

    # Verify L2-pre CDF
    l2pre_file = _find_latest_file(
        str(
            temp_datastore
            / "science/mag/l2-pre/2026/01/imap_mag_l2-pre_norm-srf_20260116_v*.cdf"
        )
    )

    with open_cdf(l2pre_file) as l2_cdf:
        b_srf = l2_cdf["b_srf"][...]

        # Row from SCIENCE fill value — should have fill values in L2-pre
        assert b_srf[SCIENCE_FILL_ROW, 0] == FILL
        assert b_srf[SCIENCE_FILL_ROW, 1] == FILL
        assert b_srf[SCIENCE_FILL_ROW, 2] == FILL

        # Row from LAYER fill value — should have fill values in L2-pre
        assert b_srf[LAYER_FILL_ROW, 0] == FILL
        assert b_srf[LAYER_FILL_ROW, 1] == FILL
        assert b_srf[LAYER_FILL_ROW, 2] == FILL
