import json
import os
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from imap_mag.cli.calibrate import gradiometry
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationLayer
from mag_toolkit.calibration.CalibrationDefinitions import CONSTANTS


@pytest.mark.skipif(
    not (os.getenv("MLM_LICENSE_FILE") or os.getenv("MLM_LICENSE_TOKEN")),
    reason="MATLAB License not set or MATLAB is not available; skipping MATLAB tests",
)
def test_gradiometry_calibration_layer_is_created_with_correct_offsets_for_one_vector(
    tmp_path, monkeypatch, temp_datastore, dynamic_work_folder
):
    gradiometry(
        start_date=datetime(2026, 9, 30),
        mode=ScienceMode.Normal,
        kappa=0.25,
        sc_interference_threshold=10.0,
    )
    layer_metadata = (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-norm-layer_20260930_v001.0001.json"
    )
    assert layer_metadata.exists()
    with open(layer_metadata) as f:
        grad_layer = json.load(f)

    assert grad_layer["method"] == "gradiometer"
    assert len(grad_layer["metadata"]["science"]) == 1
    assert (
        grad_layer["metadata"]["science"][0]
        == "imap_mag_l1c_norm-mago_20260930_v001.cdf"
    )

    layer_data = (
        temp_datastore
        / "calibration/layers/2026/09/imap_mag_gradiometer-norm-layer-data_20260930_v001.0001.csv"
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

    assert cal_layer._contents is not None
    assert cal_layer._contents[CONSTANTS.CSV_VARS.OFFSET_X][1] == -20.948437287498678
    assert cal_layer._contents[CONSTANTS.CSV_VARS.OFFSET_Y][1] == 287.80371538145209
    assert cal_layer._contents[CONSTANTS.CSV_VARS.OFFSET_Z][1] == 350.14540002089052
    assert cal_layer._contents[CONSTANTS.CSV_VARS.QUALITY_BITMASK][1] == 2
    assert cal_layer.metadata.comment == "Gradiometer layer with kappa value: 0.25"
