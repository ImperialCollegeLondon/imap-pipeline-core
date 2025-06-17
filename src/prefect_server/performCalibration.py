from datetime import datetime
from pathlib import Path

from prefect import flow

from imap_mag.api.apply import FileType, apply
from imap_mag.api.calibrate import Sensor, calibrate
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationMethod


@flow(name="calibrate", log_prints=True)
def calibrate_flow(
    date: datetime,
    method: CalibrationMethod,
    mode: ScienceMode,
    sensor: Sensor = Sensor.MAGO,
):
    return calibrate(date=date, method=method, mode=mode, sensor=sensor)


@flow(name="Calibrate and apply", log_prints=True)
def calibrate_and_apply_flow(
    date: datetime,
    method: CalibrationMethod,
    mode: ScienceMode,
    sensor: Sensor = Sensor.MAGO,
    calibration_output_type: FileType = FileType.CDF,
    L2_output_type: FileType = FileType.CDF,
):
    """
    Calibrate and apply the calibration in one flow.
    """
    # First, calibrate
    (cal_layer, science_input) = calibrate_flow(
        date=date, method=method, mode=mode, sensor=sensor
    )

    # Then, apply the calibration

    apply_flow(
        cal_layer=cal_layer,
        date=date,
        file=science_input,
        calibration_output_type=calibration_output_type,
        L2_output_type=L2_output_type,
    )


@flow(name="apply calibration", log_prints=True)
def apply_flow(
    cal_layer: Path,
    file: Path,
    date: datetime,
    calibration_output_type: FileType = FileType.CDF,
    L2_output_type: FileType = FileType.CDF,
):
    apply(
        [str(cal_layer)],
        date=date,
        input=str(file),
        calibration_output_type=calibration_output_type.value,
        l2_output_type=L2_output_type.value,
    )
