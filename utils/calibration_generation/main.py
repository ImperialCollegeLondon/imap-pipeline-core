from datetime import datetime
from enum import Enum
from typing import Annotated

import numpy as np
import typer

from calibration_generation.cal_utils import generate_ialirt_file, generate_l1d_file

app = typer.Typer()


class VALID_MATRIX_TYPES(Enum):
    IDENTITY = "identity"


class OFFSET_TYPES(Enum):
    ZERO = "zero"


def generate_ialirt_zero_offset_identity_file(version, valid_start_date, grad_value):
    i = np.eye(3, 3)

    frame_transform_mago = np.stack([i, i, i, i], axis=2)

    frame_transform_magi = frame_transform_mago.copy()

    offsets = np.zeros((2, 4, 3))

    filename = generate_ialirt_file(
        version=version,
        valid_start_date=valid_start_date,
        offsets=offsets,
        frame_transform_mago=frame_transform_mago,
        frame_transform_magi=frame_transform_magi,
        gradiometer_value=grad_value,
    )
    return filename


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
    g = gradiometer_value * np.eye(3, 3)
    fn = generate_ialirt_zero_offset_identity_file(
        version=version,
        valid_start_date=valid_start_date,
        grad_value=g,
    )
    print(f"Generated I-ALiRT Calibration file: {fn}")


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
    i = np.eye(3, 3)

    g = gradiometer_value * np.eye(3, 3)

    frame_transform_mago = np.stack([i, i, i, i], axis=2)

    frame_transform_magi = frame_transform_mago.copy()

    offsets = np.zeros((2, 4, 3))

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
