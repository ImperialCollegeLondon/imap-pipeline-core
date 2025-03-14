"""Main module."""

import logging
import subprocess
from pathlib import Path
from typing import Annotated

import typer

from imap_mag.api import apply, calibrate, process
from imap_mag.api.apiUtils import globalState
from imap_mag.api.fetch import fetch

app = typer.Typer()


@app.command()
def hello(name: str):
    print(f"Hello {name}")


@app.command()
def matlab():
    subprocess.run(["matlab", "-batch", "helloworld"])


app.command()(process.process)
app.command()(calibrate.calibrate)
app.command()(apply.apply)


@app.command()
def calibrationdemo(
    file_to_calibrate: str = typer.Argument(
        help="The file name of the file to be calibrated",
    ),
    output_file: str = typer.Argument(help="The file name of the output file"),
):
    subprocess.run(
        ["matlab", "-batch", f'demo("{file_to_calibrate}", "{output_file}")']
    )


def prepareWorkFile(file, configFile) -> Path | None:
    logging.debug(f"Grabbing file matching {file} in {configFile.source.folder}")


app.add_typer(fetch.app, name="fetch", help="Fetch data from the SDC or WebPODA")


@app.callback()
def main(verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False):
    if verbose:
        globalState["verbose"] = True


if __name__ == "__main__":
    app()  # pragma: no cover
