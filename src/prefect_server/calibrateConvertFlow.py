from datetime import date, datetime
from pathlib import Path
from typing import Annotated

from prefect import flow
from prefect.runtime import flow_run
from pydantic import Field

from imap_mag.cli.apply import FileType
from imap_mag.cli.convert import convert
from imap_mag.config import SaveMode
from imap_mag.util import ScienceMode
from mag_toolkit.calibration import ConversionStrategy
from prefect_server.constants import PREFECT_CONSTANTS


def generate_calibrate_convert_flow_run_name() -> str:
    parameters = flow_run.parameters
    input_layers: list[str] = parameters["input_layers"]
    output_layer_data_format: FileType = parameters.get(
        "output_layer_data_format", FileType.PARQUET
    )

    layers_str = ",".join(input_layers[:3])
    if len(input_layers) > 3:
        layers_str += f"...+{len(input_layers) - 3}"

    return f"Converting-{layers_str}-to-{output_layer_data_format.value}"


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.CALIBRATE_CONVERT,
    log_prints=True,
    flow_run_name=generate_calibrate_convert_flow_run_name,
)
def calibrate_convert_flow(
    input_layers: Annotated[
        list[str],
        Field(
            json_schema_extra={
                "title": "Input layers",
                "description": "Calibration layer filenames or glob patterns (e.g. '*noop*') to convert.",
                "position": 1,
            }
        ),
    ],
    start_date: Annotated[
        date | None,
        Field(
            json_schema_extra={
                "title": "Start date",
                "description": "Restrict conversion to layers on/after this date. If omitted (with end_date), all dates are searched.",
                "position": 2,
            }
        ),
    ] = None,
    end_date: Annotated[
        date | None,
        Field(
            json_schema_extra={
                "title": "End date",
                "description": "Restrict conversion to layers on/before this date (inclusive).",
                "position": 3,
            }
        ),
    ] = None,
    mode: ScienceMode | None = None,
    output_layer_data_format: FileType = FileType.PARQUET,
    output_layer_versioning_strategy: ConversionStrategy = ConversionStrategy.OVERWRITE,
    save_mode: SaveMode = SaveMode.LocalAndDatabase,
) -> list[Path]:
    return convert(
        input_layers=input_layers,
        start_date=datetime.combine(start_date, datetime.min.time())
        if start_date
        else None,
        end_date=datetime.combine(end_date, datetime.min.time()) if end_date else None,
        mode=mode,
        output_layer_data_format=output_layer_data_format,
        output_layer_versioning_strategy=output_layer_versioning_strategy,
        save_mode=save_mode,
    )
