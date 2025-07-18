from pathlib import Path

import numpy as np

from mag_toolkit.calibration import (
    CalibrationApplicator,
    CalibrationMetadata,
    Mission,
    ScienceLayer,
    ScienceValue,
)
from mag_toolkit.calibration.CalibrationDefinitions import Sensor, Validity, ValueType


def test_science_layer_calculates_magnitude_correctly():
    applicator = CalibrationApplicator()

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
    new_layer = applicator._calculate_magnitudes(science_layer)
    assert new_layer.values[0].magnitude == np.linalg.norm(science_value.value)
    assert len(new_layer.values) == 1


def test_layer_loads_science_to_full_specificity():
    sl = ScienceLayer.from_file(
        Path(
            "tests/data/science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
        )
    )
    assert sl.values[0].time == np.datetime64("2025-04-21T12:16:05.569359872", "ns")
    assert sl.values[1].time == np.datetime64("2025-04-21T12:16:06.069359872", "ns")
