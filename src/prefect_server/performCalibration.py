from datetime import datetime
from pathlib import Path

from prefect import flow
from prefect.runtime import flow_run

from imap_mag.cli.apply import FileType, apply
from imap_mag.cli.calibrate import Sensor, calibrate, gradiometry
from imap_mag.config.CalibrationConfig import CalibrationConfig
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import CalibrationLayer, CalibrationMethod
from prefect_server.constants import PREFECT_CONSTANTS


def generate_calibration_flow_run_name() -> str:
    match flow_run.flow_name:
        case PREFECT_CONSTANTS.FLOW_NAMES.GRADIOMETRY:
            method_name = CalibrationMethod.GRADIOMETER
        case _:
            method_name = flow_run.parameters["method"]

    parameters = flow_run.parameters
    date: datetime = parameters["date"]
    method: CalibrationMethod = method_name
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
    name=PREFECT_CONSTANTS.FLOW_NAMES.GRADIOMETRY,
    log_prints=True,
    flow_run_name=generate_calibration_flow_run_name,
)
def gradiometry_flow(
    date: datetime,
    mode: ScienceMode,
    kappa: float = 0.0,
    sc_interference_threshold: float = 10.0,
):
    """
    Run the gradiometry calibration.
    """

    gradiometry(
        date=date,
        mode=mode,
        kappa=kappa,
        sc_interference_threshold=sc_interference_threshold,
    )


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE,
    log_prints=True,
    flow_run_name=generate_calibration_flow_run_name,
)
def calibrate_flow(
    date: datetime,
    mode: ScienceMode,
    method: CalibrationMethod,
    configuration: CalibrationConfig | None,
    sensor: Sensor = Sensor.MAGO,
):
    return calibrate(
        date=date,
        method=method,
        mode=mode,
        sensor=sensor,
        configuration=configuration.model_dump_json() if configuration else None,
    )


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE_AND_APPLY,
    log_prints=True,
    flow_run_name=generate_calibrate_and_apply_flow_run_name,
)
def calibrate_and_apply_flow(
    date: datetime,
    method: CalibrationMethod,
    configuration: CalibrationConfig | None,
    mode: ScienceMode,
    sensor: Sensor = Sensor.MAGO,
    calibration_output_type: FileType = FileType.CDF,
    L2_output_type: FileType = FileType.CDF,
):
    """
    Calibrate and apply the calibration in one flow.
    """
    # First, calibrate
    cal_layer = calibrate_flow(
        date=date, method=method, mode=mode, sensor=sensor, configuration=configuration
    )

    layer = CalibrationLayer.from_file(cal_layer)
    science_input = layer.metadata.science[0]

    # Then, apply the calibration

    apply_flow(
        cal_layer=cal_layer,
        date=date,
        file=Path(science_input),
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
