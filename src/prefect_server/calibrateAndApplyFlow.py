from datetime import date, datetime
from pathlib import Path
from typing import Annotated

from prefect import flow
from prefect.client.schemas.objects import FlowRun
from prefect.runtime import flow_run
from pydantic import Field

from imap_mag.cli.apply import FileType, apply
from imap_mag.cli.calibrate import Sensor
from imap_mag.config import SaveMode
from imap_mag.io.file import CalibrationLayerPathHandler
from imap_mag.io.file.SPICEPathHandler import METAKERNEL_FILENAME_PREFIX
from imap_mag.util import ReferenceFrame, ScienceMode
from mag_toolkit.calibration import CalibrationLayer, CalibrationMethod, LayerDataFormat
from mag_toolkit.calibration.CalibrationConfig import (
    GradiometryConfig,
    ScienceFileVersionConfig,
    SetQualityAndNaNConfig,
)
from prefect_server.calibrationFlowCommon import (
    DEFAULT_CALIBRATION_MATRIX_VERSION,
    DEFAULT_INPUT_JSON_FILE,
    DEFAULT_MATLAB_REPO_PATH,
    PrefectScriptedL2CalibrationConfig,
    SplitByDay,
    _configuration_for_deployment,
    _days_in_range,
    _run_calibration,
    _submit_days_as_deployment_runs,
)
from prefect_server.constants import PREFECT_CONSTANTS


def generate_calibrate_and_apply_flow_run_name() -> str:
    parameters = flow_run.parameters
    start_date: datetime = parameters["start_date"]
    end_date = parameters.get("end_date")
    method: CalibrationMethod = parameters["configuration"].get_method()
    mode: ScienceMode = parameters["mode"]
    sensor: Sensor = parameters.get("sensor", Sensor.MAGO)

    date_str = start_date.strftime("%d-%m-%Y")
    if end_date and end_date != start_date:
        date_str = (
            f"{start_date.strftime('%d-%m-%Y')}-to-{end_date.strftime('%d-%m-%Y')}"
        )

    return f"Calibrating-and-applying-{date_str}-for-{sensor.value}-{mode.value}-with-{method.value}"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE_AND_APPLY,
    log_prints=True,
    flow_run_name=generate_calibrate_and_apply_flow_run_name,
)
def calibrate_and_apply_flow(
    start_date: Annotated[
        date,
        Field(
            json_schema_extra={
                "title": "Start date / single day",
                "description": "Starting content date of file(s) to be calibrated. If end date is not specified, only this day will be calibrated.",
                "position": 1,
            }
        ),
    ],
    end_date: Annotated[
        date | None,
        Field(
            json_schema_extra={
                "title": "End date",
                "description": "Ending content date of file(s) to be calibrated. If not specified, only the start date will be calibrated.",
                "position": 2,
            }
        ),
    ] = None,
    configuration: Annotated[
        PrefectScriptedL2CalibrationConfig | SetQualityAndNaNConfig | GradiometryConfig,
        Field(
            json_schema_extra={
                "title": "Calibration Type Configuration",
                "description": "Configuration to be used for the seslected calibration method.",
                "position": 3,
            }
        ),
    ] = PrefectScriptedL2CalibrationConfig(
        calibration_matrix_version=DEFAULT_CALIBRATION_MATRIX_VERSION,
        input_json_file=DEFAULT_INPUT_JSON_FILE,
        matlab_repo=DEFAULT_MATLAB_REPO_PATH,
    ),
    mode: ScienceMode = ScienceMode.Normal,
    sensor: Sensor = Sensor.MAGO,
    save_mode: SaveMode = SaveMode.LocalAndDatabase,
    metakernel: Path | None = None,
    split_by_day: SplitByDay = False,
    offset_file_output_type: FileType = FileType.CDF,
    L2_output_type: FileType = FileType.CDF,
    rotation_calibration_file_name: str | None = None,
    reference_frames: list[ReferenceFrame] | None = [
        ReferenceFrame.GSE,
        ReferenceFrame.SRF,
    ],
    offset_version_override: Annotated[
        int | None,
        Field(
            json_schema_extra={
                "title": "Offset file version override",
                "description": "Force a specific version number (1-999) for the output offset file instead of auto-incrementing. Existing file at that version is overwritten.",
            }
        ),
    ] = None,
    l2_version_override: Annotated[
        ScienceFileVersionConfig | None,
        Field(
            default=None,
            json_schema_extra={
                "title": "L2 science file version override",
                "description": "Force a specific (major, minor) version for output L2-pre science CDF files instead of auto-incrementing. Existing files at that version are overwritten.",
            },
        ),
    ] = None,
    layer_data_format: LayerDataFormat = LayerDataFormat.PARQUET,
) -> list[FlowRun] | None:
    days = _days_in_range(start_date, end_date)
    if split_by_day and len(days) > 1:
        return _submit_days_as_deployment_runs(
            deployment_name=f"{PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE_AND_APPLY}/{PREFECT_CONSTANTS.DEPLOYMENT_NAMES.CALIBRATE_AND_APPLY}",
            days=days,
            base_parameters={
                "configuration": _configuration_for_deployment(configuration),
                "mode": mode,
                "sensor": sensor,
                "offset_file_output_type": offset_file_output_type,
                "L2_output_type": L2_output_type,
                "save_mode": save_mode,
                "metakernel": metakernel,
                "rotation_calibration_file_name": rotation_calibration_file_name,
                "reference_frames": reference_frames,
                "offset_version_override": offset_version_override,
                "l2_version_override": l2_version_override,
                "layer_data_format": layer_data_format,
            },
        )

    output_files = _run_calibration(
        configuration,
        start_date,
        end_date,
        mode,
        sensor,
        save_mode,
        metakernel,
        layer_data_format,
    )

    layer_path_handlers: list[CalibrationLayerPathHandler | None] = [
        CalibrationLayerPathHandler.from_filename(path) for path in output_files
    ]
    layer_metadata_files: list[Path] = [
        layer.original_filename_or_path
        for layer in layer_path_handlers
        if layer is not None and layer.is_metadata_file()
    ]
    if metakernel is None:
        metakernel_paths = [
            path
            for path in output_files
            if path.suffix.lower() == ".tm"
            and path.name.startswith(METAKERNEL_FILENAME_PREFIX)
        ]
        if len(metakernel_paths) == 1:
            # just one metakernel so we can reuse it for apply
            metakernel = metakernel_paths[0]

    layer = CalibrationLayer.from_file(layer_metadata_files[0], load_contents=False)
    science_input = layer.metadata.science[0]
    apply(
        layers=[str(layer) for layer in layer_metadata_files],
        start_date=datetime.fromordinal(start_date.toordinal()).replace(tzinfo=None),
        end_date=datetime.fromordinal(end_date.toordinal()).replace(tzinfo=None)
        if end_date
        else None,
        input=science_input,
        offset_file_output_type=offset_file_output_type.value,
        l2_output_type=L2_output_type.value,
        save_mode=save_mode,
        mode=mode,
        spice_metakernel=metakernel,
        reference_frames=reference_frames or [],
        rotation=Path(rotation_calibration_file_name)
        if rotation_calibration_file_name
        else None,
        offset_version_override=offset_version_override,
        l2_version_override=(l2_version_override.major, l2_version_override.minor)
        if l2_version_override is not None
        else None,
    )
    return None
