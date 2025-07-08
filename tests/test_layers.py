from datetime import datetime

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
    new_layer = applicator._calculate_magnitudes(science_layer)
    assert new_layer.values[0].magnitude == np.linalg.norm(science_value.value)
    assert len(new_layer.values) == 1
