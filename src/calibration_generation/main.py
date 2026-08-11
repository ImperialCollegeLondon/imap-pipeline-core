"""Command line tool for generating and checking MAG calibration files."""

import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated

import numpy as np
import typer
from rich.console import Console

from calibration_generation import matrices, offsets, writer
from calibration_generation.matrices import MatrixSet
from calibration_generation.offsets import OffsetError
from calibration_generation.verification import (
    VerificationResult,
    verify_calibration_file,
)
from calibration_generation.writer import CalibrationFile

app = typer.Typer(
    help="Generate and check IMAP MAG calibration files.",
    no_args_is_help=True,
)

console = Console()
logger = logging.getLogger(__name__)

DEFAULT_IALIRT_GRADIOMETER_FACTOR = 0.41475
DEFAULT_SPIN_AVERAGE_FACTOR = 1.0
DEFAULT_NUMBER_OF_SPINS = 240
DEFAULT_QUALITY_FLAG_THRESHOLD = 0.0

VersionOption = Annotated[
    int,
    typer.Option("--version", prompt=True, min=0, max=999, help="File version (vNNN)."),
]
ValidStartDateOption = Annotated[
    datetime,
    typer.Option(
        "--valid-start-date",
        prompt=True,
        formats=["%Y-%m-%d", "%Y%m%d"],
        help="First date this calibration is valid for.",
    ),
]
MatricesOption = Annotated[
    MatrixSet,
    typer.Option("--matrices", prompt=True, help="Frame transform matrices to write."),
]
OutputFolderOption = Annotated[
    Path,
    typer.Option("--output-folder", help="Folder to write the file into."),
]
GradiometerOption = Annotated[
    float,
    typer.Option(
        "--gradiometer-factor",
        prompt=True,
        help="Fraction of the MAGo to MAGi delta to subtract from MAGo.",
    ),
]
SpinAverageFactorOption = Annotated[
    float,
    typer.Option(
        "--spin-average-factor",
        prompt=True,
        help="Fraction of the spin average offset to subtract in the spin plane.",
    ),
]
NumberOfSpinsOption = Annotated[
    int,
    typer.Option(
        "--number-of-spins",
        prompt=True,
        min=1,
        help="Number of spins to average the spin average offset over.",
    ),
]
QualityFlagThresholdOption = Annotated[
    float,
    typer.Option(
        "--quality-flag-threshold",
        prompt=True,
        help="MAGo to MAGi delta in nT above which data is flagged low quality.",
    ),
]
MagoOffsetsOption = Annotated[
    str | None,
    typer.Option(
        "--mago",
        help="MAGo offsets in nT as 'X,Y,Z', or four ';' separated vectors.",
    ),
]
MagiOffsetsOption = Annotated[
    str | None,
    typer.Option(
        "--magi",
        help="MAGi offsets in nT as 'X,Y,Z', or four ';' separated vectors.",
    ),
]
OffsetsFileOption = Annotated[
    Path | None,
    typer.Option(
        "--offsets-file",
        exists=True,
        dir_okay=False,
        help="YAML or JSON file of offsets, keyed by sensor name.",
    ),
]
ZeroOffsetsOption = Annotated[
    bool,
    typer.Option("--zero-offsets", help="Write zero offsets without prompting."),
]
YesOption = Annotated[
    bool,
    typer.Option("--yes", "-y", help="Write the file without asking to confirm."),
]
DryRunOption = Annotated[
    bool,
    typer.Option("--dry-run", help="Show what would be written, then stop."),
]


@app.callback()
def main(
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Log debug messages.")
    ] = False,
) -> None:
    """Set up logging for whichever command is about to run."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )


def _resolve_offsets(
    mago: str | None, magi: str | None, offsets_file: Path | None, use_zeros: bool
) -> np.ndarray:
    """Resolve offsets from the CLI options, exiting cleanly on bad input."""
    try:
        return offsets.resolve_offsets(mago, magi, offsets_file, use_zeros)
    except OffsetError as error:
        console.print(f"[red]{error}[/red]")
        raise typer.Exit(code=2) from error


def _write(
    file: CalibrationFile,
    output_folder: Path,
    matrix_set: MatrixSet,
    sensor_offsets: np.ndarray | None = None,
    confirm: bool = True,
    dry_run: bool = False,
) -> None:
    """
    Show what is about to be written, then write it unless told not to.

    Args:
        file: Calibration file contents to write.
        output_folder: Folder to write the file into.
        matrix_set: Matrix set used, for the summary.
        sensor_offsets: Offsets used, for the summary, or None if the product
            has no offsets.
        confirm: Whether to ask before writing.
        dry_run: Whether to stop after showing the summary.
    """
    console.print(f"File:       [bold]{file.filename}[/bold]")
    console.print(f"Folder:     {output_folder}")
    console.print(f"Valid from: {file.valid_start_date:%Y-%m-%d}")
    console.print(f"Matrices:   {matrices.describe(matrix_set)}")

    warnings: list[str] = []

    if sensor_offsets is not None:
        console.print(offsets.offsets_table(sensor_offsets))
        warnings = offsets.magnitude_warnings(sensor_offsets)

        if warnings:
            console.print(
                "[yellow]MAGi is closer to the spacecraft than MAGo, so its "
                "offsets are normally the larger of the two:[/yellow]"
            )
            for warning in warnings:
                console.print(f"[yellow]  ! {warning}[/yellow]")
        else:
            console.print("[green]Offset magnitudes are as expected[/green]")

    if dry_run:
        console.print("[cyan]--dry-run given, nothing written.[/cyan]")
        return

    # Anything unexpected in the summary should need an explicit yes.
    if confirm and not typer.confirm(f"Write {file.filename}?", default=not warnings):
        console.print("Nothing written.")
        raise typer.Abort()

    path = writer.write_calibration_file(file, output_folder)
    console.print(f"[green]Wrote {path}[/green]")


@app.command()
def l2(
    version: VersionOption,
    valid_start_date: ValidStartDateOption,
    matrix_set: MatricesOption = MatrixSet.LATEST,
    output_folder: OutputFolderOption = Path("."),
    yes: YesOption = False,
    dry_run: DryRunOption = False,
) -> None:
    """Generate an L2 calibration file, holding frame transform matrices."""
    mago, magi = matrices.get_frame_transforms(matrix_set)

    _write(
        writer.build_l2_file(
            version=version,
            valid_start_date=valid_start_date,
            frame_transform_mago=mago,
            frame_transform_magi=magi,
        ),
        output_folder=output_folder,
        matrix_set=matrix_set,
        confirm=not yes,
        dry_run=dry_run,
    )


@app.command()
def ialirt(
    version: VersionOption,
    valid_start_date: ValidStartDateOption,
    gradiometer_factor: GradiometerOption = DEFAULT_IALIRT_GRADIOMETER_FACTOR,
    matrix_set: MatricesOption = MatrixSet.LATEST,
    mago: MagoOffsetsOption = None,
    magi: MagiOffsetsOption = None,
    offsets_file: OffsetsFileOption = None,
    zero_offsets: ZeroOffsetsOption = False,
    output_folder: OutputFolderOption = Path("."),
    yes: YesOption = False,
    dry_run: DryRunOption = False,
) -> None:
    """Generate an I-ALiRT calibration file."""
    sensor_offsets = _resolve_offsets(mago, magi, offsets_file, zero_offsets)
    mago_transform, magi_transform = matrices.get_frame_transforms(matrix_set)

    _write(
        writer.build_ialirt_file(
            version=version,
            valid_start_date=valid_start_date,
            offsets=sensor_offsets,
            frame_transform_mago=mago_transform,
            frame_transform_magi=magi_transform,
            gradiometer_factor=gradiometer_factor,
        ),
        output_folder=output_folder,
        matrix_set=matrix_set,
        sensor_offsets=sensor_offsets,
        confirm=not yes,
        dry_run=dry_run,
    )


@app.command()
def l1d(
    version: VersionOption,
    valid_start_date: ValidStartDateOption,
    gradiometer_factor: GradiometerOption,
    spin_average_factor: SpinAverageFactorOption = DEFAULT_SPIN_AVERAGE_FACTOR,
    number_of_spins: NumberOfSpinsOption = DEFAULT_NUMBER_OF_SPINS,
    quality_flag_threshold: QualityFlagThresholdOption = (
        DEFAULT_QUALITY_FLAG_THRESHOLD
    ),
    matrix_set: MatricesOption = MatrixSet.LATEST,
    mago: MagoOffsetsOption = None,
    magi: MagiOffsetsOption = None,
    offsets_file: OffsetsFileOption = None,
    zero_offsets: ZeroOffsetsOption = False,
    output_folder: OutputFolderOption = Path("."),
    yes: YesOption = False,
    dry_run: DryRunOption = False,
) -> None:
    """Generate an L1d calibration file."""
    sensor_offsets = _resolve_offsets(mago, magi, offsets_file, zero_offsets)
    mago_transform, magi_transform = matrices.get_frame_transforms(matrix_set)

    _write(
        writer.build_l1d_file(
            version=version,
            valid_start_date=valid_start_date,
            offsets=sensor_offsets,
            frame_transform_mago=mago_transform,
            frame_transform_magi=magi_transform,
            gradiometer_factor=gradiometer_factor,
            spin_average_factor=spin_average_factor,
            number_of_spins=number_of_spins,
            quality_flag_threshold=quality_flag_threshold,
        ),
        output_folder=output_folder,
        matrix_set=matrix_set,
        sensor_offsets=sensor_offsets,
        confirm=not yes,
        dry_run=dry_run,
    )


def _report(path: Path, result: VerificationResult) -> None:
    """Print the outcome of checking one calibration file."""
    console.print(f"\n[bold]{path.name}[/bold] ({result.level} calibration)")

    for value in result.summary:
        console.print(f"  {value}")
    for warning in result.warnings:
        console.print(f"  [yellow]! {warning}[/yellow]")
    for error in result.errors:
        console.print(f"  [red]x {error}[/red]")

    if result.passed:
        console.print("  [green]All checks passed[/green]")
    else:
        console.print("  [red]Checks failed[/red]")


@app.command()
def verify(
    files: Annotated[
        list[Path],
        typer.Argument(exists=True, dir_okay=False, help="Calibration files to check."),
    ],
) -> None:
    """Check that generated calibration files hold what they should."""
    failed = False

    for path in files:
        try:
            result = verify_calibration_file(path)
        except ValueError as error:
            console.print(f"[red]{error}[/red]")
            failed = True
            continue

        _report(path, result)
        failed = failed or not result.passed

    if failed:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()  # pragma: no cover
