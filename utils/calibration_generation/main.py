import re
from datetime import datetime
from enum import Enum
from typing import Annotated

import numpy as np
import typer
from rich.prompt import FloatPrompt
from spacepy import pycdf

from calibration_generation.cal_utils import (
    generate_ialirt_file,
    generate_l1d_file,
    generate_l2_file,
    get_correctly_arranged_offsets,
    get_one_sensor_stacked_offsets,
    get_stacked_identity_matrices,
    verify_frame_transforms,
    verify_offsets,
)

app = typer.Typer()


class VALID_MATRIX_TYPES(Enum):
    IDENTITY = "identity"


class OFFSET_TYPES(Enum):
    ZERO = "zero"
    CUSTOM = "custom"


def get_offsets(offset_type: OFFSET_TYPES) -> np.ndarray:
    match offset_type:
        case OFFSET_TYPES.ZERO:
            return np.zeros((2, 4, 3))
        case OFFSET_TYPES.CUSTOM:
            x_offset = FloatPrompt.ask("MAGo X Offset")
            y_offset = FloatPrompt.ask("MAGo Y Offset")
            z_offset = FloatPrompt.ask("MAGo Z Offset")
            mago_offsets = get_one_sensor_stacked_offsets(x_offset, y_offset, z_offset)
            x_offset = FloatPrompt.ask("MAGi X Offset")
            y_offset = FloatPrompt.ask("MAGi Y Offset")
            z_offset = FloatPrompt.ask("MAGi Z Offset")
            magi_offsets = get_one_sensor_stacked_offsets(x_offset, y_offset, z_offset)
            return get_correctly_arranged_offsets(mago_offsets, magi_offsets)
        case _:
            raise ValueError(f"Unknown offset type: {offset_type}")


@app.command()
def verify(filename: Annotated[str, typer.Option(prompt=True)]):
    calibration_type = re.match(r"imap_mag_(\w+)-calibration", filename)
    if calibration_type is None:
        raise ValueError(
            f"Filename does not match expected pattern: {filename}. Expected 'imap_mag_<type>-calibration_YYYYMMDD_vXXX.cdf'"
        )
    valid = True
    with pycdf.CDF(filename) as cdf:
        match calibration_type.group(1):
            case "ialirt":
                print("Verifying I-ALiRT calibration file...")
                valid = valid and verify_frame_transforms(cdf) and verify_offsets(cdf)
            case "l2":
                print("Verifying L2 calibration file...")
                valid = valid and verify_frame_transforms(cdf)
            case "l1d":
                print("Verifying L1d calibration file...")
                valid = valid and verify_frame_transforms(cdf) and verify_offsets(cdf)
            case _:
                raise ValueError(
                    f"Unknown calibration type in filename: {calibration_type.group(1)}"
                )
    if valid:
        print(f"Validation of {filename} successful. All checks passed.")
    else:
        print(f"Validation of {filename} failed. See errors above.")


@app.command()
def ialirt(
    version: Annotated[int, typer.Option(prompt=True)],
    valid_start_date: Annotated[datetime, typer.Option(prompt=True)],
    gradiometer_value: Annotated[float, typer.Option(prompt=True)],
    matrix_type: Annotated[
        VALID_MATRIX_TYPES, typer.Option(prompt=True, help="Type of matrix to generate")
    ],
    offset_type: Annotated[
        OFFSET_TYPES,
        typer.Option(
            prompt=True, help="Type of offset to generate (e.g., 'zero', 'random')"
        ),
    ],
):
    offsets = get_offsets(offset_type)
    g = gradiometer_value * np.eye(3, 3)
    fn = generate_ialirt_file(
        version=version,
        valid_start_date=valid_start_date,
        offsets=offsets,
        frame_transform_mago=get_stacked_identity_matrices(),
        frame_transform_magi=get_stacked_identity_matrices(),
        gradiometer_value=g,
    )
    print(f"Generated I-ALiRT Calibration file: {fn}")


@app.command()
def l2(
    version: Annotated[int, typer.Option(prompt=True)],
    valid_start_date: Annotated[datetime, typer.Option(prompt=True)],
    matrix_type: Annotated[
        VALID_MATRIX_TYPES, typer.Option(prompt=True, help="Type of matrix to generate")
    ],
):
    frame_transform_mago = get_stacked_identity_matrices()

    frame_transform_magi = get_stacked_identity_matrices()

    fn = generate_l2_file(
        version=version,
        valid_start_date=valid_start_date,
        frame_transform_magi=frame_transform_magi,
        frame_transform_mago=frame_transform_mago,
    )
    print(f"Generated L2 Calibration file: {fn}")


@app.command()
def l1d(
    version: Annotated[int, typer.Option(prompt=True)],
    valid_start_date: Annotated[datetime, typer.Option(prompt=True)],
    gradiometer_value: Annotated[float, typer.Option(prompt=True)],
    spin_average_value: Annotated[float, typer.Option(prompt=True)],
    spin_number_cycles: Annotated[int, typer.Option(prompt=True)],
    quality_threshold_value: Annotated[float, typer.Option(prompt=True)],
    matrix_type: Annotated[
        VALID_MATRIX_TYPES, typer.Option(prompt=True, help="Type of matrix to generate")
    ],
    offset_type: Annotated[
        OFFSET_TYPES,
        typer.Option(prompt=True, help="Type of offset to generate (e.g., 'zero')"),
    ],
):
    g = gradiometer_value * np.eye(3, 3)

    frame_transform_mago = get_stacked_identity_matrices()

    frame_transform_magi = get_stacked_identity_matrices()

    offsets = get_offsets(offset_type)

    filename = generate_l1d_file(
        version=version,
        valid_start_date=valid_start_date,
        offsets=offsets,
        frame_transform_mago=frame_transform_mago,
        frame_transform_magi=frame_transform_magi,
        gradiometer_value=g,
        spin_average_factor=spin_average_value,
        spin_num_cycles=spin_number_cycles,
        quality_flag_threshold=quality_threshold_value,
    )
    print(f"Generated L1d Calibration file: {filename}")


if __name__ == "__main__":
    app()
