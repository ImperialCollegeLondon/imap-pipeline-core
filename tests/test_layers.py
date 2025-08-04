import json
from pathlib import Path

import numpy as np
import pandas as pd
from spacepy import pycdf

from mag_toolkit.calibration import (
    CalibrationMetadata,
    Mission,
    ScienceLayer,
    ScienceValue,
)
from mag_toolkit.calibration.CalibrationDefinitions import Sensor, Validity, ValueType
from tests.util.miscellaneous import DATASTORE


def test_science_layer_calculates_magnitude_correctly():
    science_value = ScienceValue(
        time=np.datetime64("2025-01-01T12:00"), value=[1, 1, 1], range=3
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
        values=[science_value],
    )
    new_layer = science_layer.calculate_magnitudes()
    assert new_layer.values[0].magnitude == np.linalg.norm(science_value.value)
    assert len(new_layer.values) == 1


def test_layer_loads_science_to_full_specificity():
    sl = ScienceLayer.from_file(
        DATASTORE / "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )
    assert sl.values[0].time == np.datetime64("2025-04-21T12:16:05.569359872", "ns")
    assert sl.values[1].time == np.datetime64("2025-04-21T12:16:06.069359872", "ns")


def test_layer_writes_science_to_full_specificity():
    sl = ScienceLayer.from_file(
        DATASTORE / "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
    )
    test_science_layer_path = Path("output/test-science-layer.json")
    sl._write_to_json(test_science_layer_path)

    with open(test_science_layer_path) as f:
        layer = json.load(f)

    assert layer["values"][0]["time"] == "2025-04-21T12:16:05.569359872"
    assert layer["values"][1]["time"] == "2025-04-21T12:16:06.069359872"


def test_science_layer_writes_to_cdf_correctly(tmp_path):
    # Create a sample ScienceLayer
    science_value = ScienceValue(
        time=np.datetime64("2025-01-01T12:00"), value=[1, 1, 1], range=3
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
        values=[science_value],
    )
    cdf_path = tmp_path / "test_layer.cdf"
    science_layer.calculate_magnitudes()  # Ensure magnitudes are calculated
    science_layer.writeToFile(cdf_path)

    with pycdf.CDF(str(cdf_path)) as cdf_file:
        vecs = cdf_file["vectors"][...]
        assert vecs is not None
        assert vecs[0][0] == science_layer.values[0].value[0]
        assert vecs[0][1] == science_layer.values[0].value[1]
        assert vecs[0][2] == science_layer.values[0].value[2]
        assert np.datetime64(cdf_file["epoch"][0]) == science_layer.values[0].time  # type: ignore
        assert cdf_file.attrs["Mission_group"][0] == science_layer.mission.value


def test_science_layer_writes_to_csv(tmp_path):
    science_value = ScienceValue(
        time=np.datetime64("2025-01-01T12:00:00.056789"), value=[1, 1, 1], range=3
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
        values=[science_value],
    )
    csv_path = tmp_path / "test_layer.csv"
    science_layer.calculate_magnitudes()
    science_layer.writeToFile(csv_path)

    df = pd.read_csv(csv_path, parse_dates=["epoch"])
    assert df.x.iloc[0] == science_layer.values[0].value[0]
    assert df.y.iloc[0] == science_layer.values[0].value[1]
    assert df.z.iloc[0] == science_layer.values[0].value[2]
    assert df.magnitude.iloc[0] == np.linalg.norm(science_layer.values[0].value)
    assert df.epoch.iloc[0] == science_layer.values[0].time
