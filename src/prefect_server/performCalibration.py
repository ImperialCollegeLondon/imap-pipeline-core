from datetime import datetime
from pathlib import Path

from prefect import flow
from prefect.runtime import flow_run

from imap_mag.cli.apply import FileType, apply
from imap_mag.cli.calibrate import Sensor, calibrate, gradiometry
from imap_mag.config import SaveMode
from imap_mag.config.CalibrationConfig import CalibrationConfig
from imap_mag.util import ReferenceFrame, ScienceMode
from mag_toolkit.calibration import CalibrationLayer, CalibrationMethod
from prefect_server.constants import PREFECT_CONSTANTS


def generate_calibration_flow_run_name() -> str:
    match flow_run.flow_name:
        case PREFECT_CONSTANTS.FLOW_NAMES.GRADIOMETRY:
            method_name = CalibrationMethod.GRADIOMETER
        case _:
            method_name = flow_run.parameters["method"]

    parameters = flow_run.parameters
    start_date: datetime = parameters["start_date"]
    end_date = parameters.get("end_date")
    method: CalibrationMethod = method_name
    mode: ScienceMode = parameters["mode"]
    sensor: Sensor = parameters.get("sensor", Sensor.MAGO)

    date_str = start_date.strftime("%d-%m-%Y")
    if end_date and end_date != start_date:
        date_str = (
            f"{start_date.strftime('%d-%m-%Y')}-to-{end_date.strftime('%d-%m-%Y')}"
        )

    return f"Calibrating-{date_str}-for-{sensor.value}-{mode.value}-with-{method.value}"


def generate_apply_calibration_flow_run_name() -> str:
    parameters = flow_run.parameters
    layers: list[str] = parameters["layers"]
    start_date: datetime = parameters["start_date"]
    end_date = parameters.get("end_date")

    layers_str = ",".join(layers[:3])
    if len(layers) > 3:
        layers_str += f"...+{len(layers) - 3}"

    date_str = start_date.strftime("%d-%m-%Y")
    if end_date and end_date != start_date:
        date_str = (
            f"{start_date.strftime('%d-%m-%Y')}-to-{end_date.strftime('%d-%m-%Y')}"
        )

    return f"Applying-{layers_str}-for-{date_str}"


def generate_calibrate_and_apply_flow_run_name() -> str:
    parameters = flow_run.parameters
    start_date: datetime = parameters["start_date"]
    end_date = parameters.get("end_date")
    method: CalibrationMethod = parameters["method"]
    mode: ScienceMode = parameters["mode"]
    sensor: Sensor = parameters.get("sensor", Sensor.MAGO)

    date_str = start_date.strftime("%d-%m-%Y")
    if end_date and end_date != start_date:
        date_str = (
            f"{start_date.strftime('%d-%m-%Y')}-to-{end_date.strftime('%d-%m-%Y')}"
        )

    return f"Calibrating-and-applying-{date_str}-for-{sensor.value}-{mode.value}-with-{method.value}"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.GRADIOMETRY,
    log_prints=True,
    flow_run_name=generate_calibration_flow_run_name,
)
def gradiometry_flow(
    start_date: datetime,
    mode: ScienceMode,
    kappa: float = 0.0,
    sc_interference_threshold: float = 10.0,
):
    """
    Run the gradiometry calibration.
    """

    gradiometry(
        start_date=start_date,
        mode=mode,
        kappa=kappa,
        sc_interference_threshold=sc_interference_threshold,
        save_mode=SaveMode.LocalAndDatabase,
    )


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE,
    log_prints=True,
    flow_run_name=generate_calibration_flow_run_name,
)
def calibrate_flow(
    start_date: datetime,
    end_date: datetime | None = None,
    method: CalibrationMethod = CalibrationMethod.KEPKO,
    mode: ScienceMode = ScienceMode.Normal,
    configuration: CalibrationConfig | None = None,
    sensor: Sensor = Sensor.MAGO,
    save_mode: SaveMode = SaveMode.LocalAndDatabase,
) -> list[Path]:
    """Calibrate for a date or date range. Returns a list of calibration layer paths."""
    return calibrate(
        start_date=start_date,
        end_date=end_date,
        method=method,
        mode=mode,
        sensor=sensor,
        configuration=configuration.model_dump_json() if configuration else None,
        save_mode=save_mode,
    )


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE_AND_APPLY,
    log_prints=True,
    flow_run_name=generate_calibrate_and_apply_flow_run_name,
)
def calibrate_and_apply_flow(
    start_date: datetime,
    end_date: datetime | None = None,
    method: CalibrationMethod = CalibrationMethod.KEPKO,
    configuration: CalibrationConfig | None = None,
    mode: ScienceMode = ScienceMode.Normal,
    sensor: Sensor = Sensor.MAGO,
    offset_file_output_type: FileType = FileType.CDF,
    L2_output_type: FileType = FileType.CDF,
    save_mode: SaveMode = SaveMode.LocalAndDatabase,
):
    """
    Calibrate and apply the calibration in one flow, for a date or date range.
    """
    cal_layer_paths: list[Path] = calibrate(
        start_date=start_date,
        end_date=end_date,
        method=method,
        mode=mode,
        sensor=sensor,
        configuration=configuration.model_dump_json() if configuration else None,
        save_mode=save_mode,
    )

    for cal_layer_path in cal_layer_paths:
        layer = CalibrationLayer.from_file(cal_layer_path)
        science_input = layer.metadata.science[0]

        apply(
            layers=[str(cal_layer_path)],
            start_date=layer.metadata.content_date.astype(datetime),
            input=science_input,
            offset_file_output_type=offset_file_output_type.value,
            l2_output_type=L2_output_type.value,
            save_mode=save_mode,
            mode=mode,
        )


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.APPLY_CALIBRATION,
    log_prints=True,
    flow_run_name=generate_apply_calibration_flow_run_name,
)
def apply_flow(
    layers: list[str],
    start_date: datetime,
    end_date: datetime | None = None,
    mode: ScienceMode | None = None,
    science_input_file: str | None = None,
    offset_file_output_type: FileType = FileType.CDF,
    L2_output_type: FileType = FileType.CDF,
    save_mode: SaveMode = SaveMode.LocalAndDatabase,
    rotation_calibration_file_name: str | None = None,
    spice_metakernel: Path | None = None,
    reference_frames: list[ReferenceFrame] | None = [
        ReferenceFrame.GSE,
        ReferenceFrame.SRF,
    ],
):
    """Apply calibration layers for a date or date range.

    Args:
        layers: Layer filenames or glob patterns (e.g. ["*noop*"], ["*"]).
        start_date: Start date for processing.
        end_date: End date (inclusive). If None, only start_date is processed.
        mode: Science mode (norm/burst) for discovering science files when file is None.
        science_input_file: Science filename. If None, discovered using mode and date.
        save_mode: Where to save output files.
    """
    apply(
        layers,
        start_date=start_date,
        end_date=end_date,
        mode=mode,
        input=science_input_file,
        offset_file_output_type=offset_file_output_type.value,
        l2_output_type=L2_output_type.value,
        save_mode=save_mode,
        rotation=Path(rotation_calibration_file_name)
        if rotation_calibration_file_name
        else None,
        spice_metakernel=spice_metakernel,
        reference_frames=reference_frames,
    )
