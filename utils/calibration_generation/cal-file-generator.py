from datetime import datetime
from typing import Annotated

import numpy as np
import typer

from calibration_generation.cal_utils import generate_ialirt_file

app = typer.Typer()


@app.command()
def l1d(
    project_name: Annotated[str, typer.Option(prompt=True)],
):
    print(f"Prompted for {project_name}")


@app.command()
def ialirt(
    version: Annotated[int, typer.Option(prompt=True)],
    valid_start_date: Annotated[datetime, typer.Option(prompt=True)],
):
    i = np.eye(3, 3)
    frame_transform_magi = np.stack([i, i, i, i], axis=2)

    frame_transform_mago = np.stack([i, i, i, i], axis=2)

    offsets = np.zeros((2, 4, 3))
    filename = generate_ialirt_file(
        version=version,
        valid_start_date=valid_start_date,
        offsets=offsets,
        frame_transform_mago=frame_transform_mago,
        frame_transform_magi=frame_transform_magi,
    )
    print(f"Generated I-ALiRT Calibration file: {filename}")


if __name__ == "__main__":
    app()
