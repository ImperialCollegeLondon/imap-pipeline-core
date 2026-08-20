from datetime import date, datetime
from pathlib import Path
from typing import Annotated

from prefect import flow
from prefect.client.schemas.objects import FlowRun
from prefect.runtime import flow_run
from pydantic import Field

from imap_mag.cli.apply import FileType, apply
from imap_mag.config import SaveMode
from imap_mag.util import ReferenceFrame, ScienceMode
from mag_toolkit.calibration.CalibrationConfig import ScienceFileVersionConfig
from prefect_server.calibrationFlowCommon import (
    SplitByDay,
    _days_in_range,
    _submit_days_as_deployment_runs,
)
from prefect_server.constants import PREFECT_CONSTANTS


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


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.APPLY_CALIBRATION,
    log_prints=True,
    flow_run_name=generate_apply_calibration_flow_run_name,
)
def apply_flow(
    layers: list[str],
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
    split_by_day: SplitByDay = False,
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
) -> list[FlowRun] | None:
    days = _days_in_range(start_date, end_date)
    if split_by_day and len(days) > 1:
        return _submit_days_as_deployment_runs(
            deployment_name=f"{PREFECT_CONSTANTS.FLOW_NAMES.APPLY_CALIBRATION}/{PREFECT_CONSTANTS.DEPLOYMENT_NAMES.APPLY_CALIBRATION}",
            days=days,
            base_parameters={
                "layers": layers,
                "mode": mode,
                "science_input_file": science_input_file,
                "offset_file_output_type": offset_file_output_type,
                "L2_output_type": L2_output_type,
                "save_mode": save_mode,
                "rotation_calibration_file_name": rotation_calibration_file_name,
                "spice_metakernel": spice_metakernel,
                "reference_frames": reference_frames,
                "offset_version_override": offset_version_override,
                "l2_version_override": l2_version_override,
            },
        )

    apply(
        layers,
        start_date=datetime.fromordinal(start_date.toordinal()).replace(tzinfo=None),
        end_date=datetime.fromordinal(end_date.toordinal()).replace(tzinfo=None)
        if end_date
        else None,
        mode=mode,
        input=science_input_file,
        offset_file_output_type=offset_file_output_type.value,
        l2_output_type=L2_output_type.value,
        save_mode=save_mode,
        rotation=Path(rotation_calibration_file_name)
        if rotation_calibration_file_name
        else None,
        spice_metakernel=spice_metakernel,
        reference_frames=reference_frames or [],
        offset_version_override=offset_version_override,
        l2_version_override=(l2_version_override.major, l2_version_override.minor)
        if l2_version_override is not None
        else None,
    )

    return None
