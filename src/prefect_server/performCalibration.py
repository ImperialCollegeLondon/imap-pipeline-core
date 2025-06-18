from datetime import datetime
from pathlib import Path

from prefect import flow
from prefect.runtime import flow_run

from imap_mag.api.apply import FileType, apply
from imap_mag.api.calibrate import Sensor, calibrate
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationMethod
from prefect_server.constants import CONSTANTS as PREFECT_CONSTANTS


def generate_calibration_flow_run_name() -> str:
    parameters = flow_run.parameters
    date: datetime = parameters["date"]
    method: CalibrationMethod = parameters["method"]
    mode: ScienceMode = parameters["mode"]
    sensor: Sensor = parameters.get("sensor", Sensor.MAGO)

    return f"Calibrating-{date.strftime('%d-%m-%Y')}-for-{sensor.value}-{mode.value}-with-{method.value}"


def generate_apply_calibration_flow_run_name() -> str:
    parameters = flow_run.parameters
    cal_layer: Path = parameters["cal_layer"]
    file: Path = parameters["file"]
    date: datetime = parameters["date"]

    return f"Applying-calibration-{cal_layer.name}-to-{file.name}-for-{date.strftime('%d-%m-%Y')}"


def generate_calibrate_and_apply_flow_run_name() -> str:
    parameters = flow_run.parameters
    date: datetime = parameters["date"]
    method: CalibrationMethod = parameters["method"]
    mode: ScienceMode = parameters["mode"]
    sensor: Sensor = parameters.get("sensor", Sensor.MAGO)

    return f"Calibrating-and-applying-{date.strftime('%d-%m-%Y')}-for-{sensor.value}-{mode.value}-with-{method.value}"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE,
    log_prints=True,
    flow_run_name=generate_calibration_flow_run_name,
)
def calibrate_flow(
    date: datetime,
    method: CalibrationMethod,
    mode: ScienceMode,
    sensor: Sensor = Sensor.MAGO,
):
    return calibrate(date=date, method=method, mode=mode, sensor=sensor)


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE_AND_APPLY,
    log_prints=True,
    flow_run_name=generate_calibrate_and_apply_flow_run_name,
)
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


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.APPLY_CALIBRATION,
    log_prints=True,
    flow_run_name=generate_apply_calibration_flow_run_name,
)
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
