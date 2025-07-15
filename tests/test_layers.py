from datetime import datetime

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


def test_science_layer_calculates_magnitude_correctly():
    science_value = ScienceValue(
        time=datetime(2025, 1, 1, 12, 0, 0), value=[1, 1, 1], range=3
    )
    science_layer = ScienceLayer(
        id="",
        mission=Mission.IMAP,
        validity=Validity(
            start=datetime(2025, 1, 1, 12, 0, 0), end=datetime(2025, 1, 1, 12, 0, 0)
        ),
        sensor=Sensor.MAGO,
        version=0,
        metadata=CalibrationMetadata(
            dependencies=[],
            science=[],
            creation_timestamp=datetime(2025, 7, 7),
        ),
        value_type=ValueType.VECTOR,
        science_file="imap_mag_l1c_mago-norm_v000.cdf",
        values=[science_value],
    )
    new_layer = science_layer.calculate_magnitudes()
    assert new_layer.values[0].magnitude == np.linalg.norm(science_value.value)
    assert len(new_layer.values) == 1


def test_science_layer_writes_to_cdf_correctly(tmp_path):
    # Create a sample ScienceLayer
    science_value = ScienceValue(
        time=datetime(2025, 1, 1, 12, 0, 0), value=[1, 1, 1], range=3
    )
    science_layer = ScienceLayer(
        id="test_layer",
        mission=Mission.IMAP,
        validity=Validity(
            start=datetime(2025, 1, 1, 12, 0, 0), end=datetime(2025, 1, 1, 12, 0, 0)
        ),
        sensor=Sensor.MAGO,
        version=0,
        metadata=CalibrationMetadata(
            dependencies=[],
            science=[],
            creation_timestamp=datetime(2025, 7, 7),
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
        assert cdf_file["epoch"][0] == science_layer.values[0].time
        assert cdf_file.attrs["Mission_group"][0] == science_layer.mission.value


def test_science_layer_writes_to_csv(tmp_path):
    science_value = ScienceValue(
        time=datetime(2025, 1, 1, 12, 0, 0, 56789), value=[1, 1, 1], range=3
    )
    science_layer = ScienceLayer(
        id="test_layer",
        mission=Mission.IMAP,
        validity=Validity(
            start=datetime(2025, 1, 1, 12, 0, 0), end=datetime(2025, 1, 1, 12, 0, 0)
        ),
        sensor=Sensor.MAGO,
        version=0,
        metadata=CalibrationMetadata(
            dependencies=[],
            science=[],
            creation_timestamp=datetime(2025, 7, 7),
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
