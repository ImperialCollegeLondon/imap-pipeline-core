import json
import re
import shutil

import numpy as np
import pandas as pd
import pytest
from spacepy import pycdf

from mag_toolkit.calibration import (
    CalibrationLayer,
    CalibrationMetadata,
    CalibrationMethod,
    Mission,
    ScienceLayer,
    ScienceValue,
)
from mag_toolkit.calibration.CalibrationDefinitions import (
    CONSTANTS,
    Sensor,
    Validity,
    ValueType,
)
from tests.util.miscellaneous import DATASTORE, TEST_DATA


def test_science_layer_calculates_magnitude_correctly():
    science_value = ScienceValue(
        time=np.datetime64("2025-01-01T12:00"), value=[1, 1, 1], range=3
    )
    contents = pd.DataFrame(
        {
            CONSTANTS.CSV_VARS.EPOCH: [science_value.time],
            CONSTANTS.CSV_VARS.X: [science_value.value[0]],
            CONSTANTS.CSV_VARS.Y: [science_value.value[1]],
            CONSTANTS.CSV_VARS.Z: [science_value.value[2]],
            CONSTANTS.CSV_VARS.RANGE: [science_value.range],
            CONSTANTS.CSV_VARS.QUALITY_FLAG: [science_value.quality_flag],
            CONSTANTS.CSV_VARS.QUALITY_BITMASK: [science_value.quality_bitmask],
        }
    )
    science_layer = ScienceLayer(
        id="",
        mission=Mission.IMAP,
        validity=Validity(
            start=np.datetime64("2025-01-01T12:00"),
            end=np.datetime64("2025-01-01T12:00"),
        ),
        sensor=Sensor.MAGO,
        version=0,
        metadata=CalibrationMetadata(
            dependencies=[],
            science=[],
            creation_timestamp=np.datetime64("2025-07-07"),
        ),
        value_type=ValueType.VECTOR,
        science_file="imap_mag_l1c_mago-norm_v000.cdf",
    )
    science_layer._contents = contents
    new_layer = science_layer.calculate_magnitudes()
    assert new_layer._contents is not None
    assert new_layer._contents["magnitude"][0] == np.linalg.norm(science_value.value)
    assert len(new_layer._contents) == 1


def test_layer_loads_science_to_full_specificity():
    sl = ScienceLayer.from_file(
        DATASTORE / "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf",
        load_contents=True,
    )
    assert sl._contents is not None
    assert sl._contents[CONSTANTS.CSV_VARS.EPOCH][0] == np.datetime64(
        "2025-04-21T12:16:05.569359872", "ns"
    )
    assert sl._contents[CONSTANTS.CSV_VARS.EPOCH][1] == np.datetime64(
        "2025-04-21T12:16:06.069359872", "ns"
    )


def test_layer_writes_science_to_full_specificity(tmp_path):
    sl = ScienceLayer.from_file(
        DATASTORE / "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf",
        load_contents=True,
    )
    test_science_layer_path = tmp_path / "test-science-layer.json"
    sl._write_to_json(test_science_layer_path)

    with open(test_science_layer_path) as f:
        layer = json.load(f)

    assert layer["values"][0]["time"] == "2025-04-21T12:16:05.569359872"
    assert layer["values"][1]["time"] == "2025-04-21T12:16:06.069359872"


def test_science_layer_writes_to_cdf_correctly(tmp_path):
    # Create a sample ScienceLayer

    contents = pd.DataFrame(
        {
            CONSTANTS.CSV_VARS.EPOCH: [np.datetime64("2025-01-01T12:00")],
            CONSTANTS.CSV_VARS.X: [1],
            CONSTANTS.CSV_VARS.Y: [1],
            CONSTANTS.CSV_VARS.Z: [1],
            CONSTANTS.CSV_VARS.RANGE: [3],
        }
    )
    science_layer = ScienceLayer(
        id="test_layer",
        mission=Mission.IMAP,
        validity=Validity(
            start=np.datetime64("2025-01-01T12:00"),
            end=np.datetime64("2025-01-01T12:00"),
        ),
        sensor=Sensor.MAGO,
        version=0,
        metadata=CalibrationMetadata(
            dependencies=[],
            science=[],
            creation_timestamp=np.datetime64("2025-07-07"),
        ),
        value_type=ValueType.VECTOR,
        science_file="imap_mag_l1c_mago-norm_v000.cdf",
    )
    science_layer._contents = contents
    cdf_path = tmp_path / "test_layer.cdf"
    science_layer.calculate_magnitudes()  # Ensure magnitudes are calculated
    science_layer.writeToFile(cdf_path)

    with pycdf.CDF(str(cdf_path)) as cdf_file:
        vecs = cdf_file["vectors"][...]
        assert vecs is not None
        assert vecs[0][0] == science_layer._contents["x"][0]
        assert vecs[0][1] == science_layer._contents["y"][0]
        assert vecs[0][2] == science_layer._contents["z"][0]
        assert np.datetime64(cdf_file["epoch"][0]) == science_layer._contents.time[0]  # type: ignore
        assert cdf_file.attrs["Mission_group"][0] == science_layer.mission.value


def test_science_layer_writes_to_csv(tmp_path):
    contents = pd.DataFrame(
        {
            CONSTANTS.CSV_VARS.EPOCH: [np.datetime64("2025-01-01T12:00")],
            CONSTANTS.CSV_VARS.X: [1],
            CONSTANTS.CSV_VARS.Y: [1],
            CONSTANTS.CSV_VARS.Z: [1],
            CONSTANTS.CSV_VARS.RANGE: [3],
        }
    )
    science_layer = ScienceLayer(
        id="test_layer",
        mission=Mission.IMAP,
        validity=Validity(
            start=np.datetime64("2025-01-01T12:00"),
            end=np.datetime64("2025-01-01T12:00"),
        ),
        sensor=Sensor.MAGO,
        version=0,
        metadata=CalibrationMetadata(
            dependencies=[],
            science=[],
            creation_timestamp=np.datetime64("2025-07-07"),
        ),
        value_type=ValueType.VECTOR,
        science_file="imap_mag_l1c_mago-norm_v000.cdf",
    )
    science_layer._contents = contents
    csv_path = tmp_path / "test_layer.csv"
    science_layer.calculate_magnitudes()
    science_layer.writeToFile(csv_path)

    df = pd.read_csv(csv_path, parse_dates=["time"])
    assert df.x.iloc[0] == science_layer._contents.x.iloc[0]
    assert df.y.iloc[0] == science_layer._contents.y.iloc[0]
    assert df.z.iloc[0] == science_layer._contents.z.iloc[0]
    assert df.magnitude.iloc[0] == np.linalg.norm(
        science_layer._contents[["x", "y", "z"]].iloc[0]
    )
    assert df.time.iloc[0] == science_layer._contents.time[0]


def test_calibration_layer_loads_csv_correctly():
    # Set up.
    calibration_layer = (
        DATASTORE
        / "calibration/layers/2025/10/imap_mag_noop-layer-data_20251017_v001.csv"
    )

    # Exercise.
    cl = CalibrationLayer.from_file(calibration_layer)

    # Verify metadata.
    assert cl.mission == Mission.IMAP
    assert cl.validity.start == np.datetime64("2025-10-17T02:11:51.521309000", "ns")
    assert cl.validity.end == np.datetime64("2025-10-17T02:11:59.021309000", "ns")
    assert cl.sensor == Sensor.MAGO
    assert cl.version == 0
    assert cl.metadata.dependencies == []
    assert cl.metadata.science == []
    assert cl.metadata.data_filename == calibration_layer
    assert cl.metadata.creation_timestamp is not None
    assert cl.value_type == ValueType.VECTOR
    assert cl.method == CalibrationMethod.NOOP
    assert cl._contents is not None

    # Verify values.
    assert len(cl._contents) == 16
    assert cl._contents.time[0] == np.datetime64("2025-10-17T02:11:51.521309000", "ns")
    assert cl._contents.time[-1] == np.datetime64("2025-10-17T02:11:59.021309000", "ns")


def test_calibration_layer_warning_on_overwriting_existing_data(
    tmp_path, capture_cli_logs
):
    # Set up.
    metadata_file = TEST_DATA / "metadata_file_with_values_and_data_file.json"
    data_file = (
        DATASTORE
        / "calibration/layers/2025/10/imap_mag_noop-layer-data_20251017_v001.csv"
    )

    shutil.copy(metadata_file, tmp_path / "metadata_file.json")
    shutil.copy(data_file, tmp_path / "data_file.csv")

    # Exercise.
    cl = CalibrationLayer.from_file(tmp_path / "metadata_file.json")

    # Verify.
    assert (
        f"Existing calibration values will be overwritten with data in {tmp_path / 'data_file.csv'!s}."
        in capture_cli_logs.text
    )

    assert len(cl.values) == 16


def test_calibration_layer_error_on_loading_empty_csv(tmp_path):
    # Set up.
    empty_csv = tmp_path / "empty.csv"
    empty_csv.touch()

    empty_csv.write_text(
        "time,offset_x,offset_y,offset_z,timedelta,quality_flag,quality_bitmask\n"
    )

    # Exercise and verify.
    with pytest.raises(
        ValueError, match=re.escape("CSV file is empty or does not contain valid data")
    ):
        CalibrationLayer._from_csv(empty_csv)
