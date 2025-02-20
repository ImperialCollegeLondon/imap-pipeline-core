"""Main module."""

import subprocess
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

app.add_typer(fetch.app, name="fetch", help="Fetch data from the SDC or WebPODA")


@app.callback()
def main(verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False):
    if verbose:
        globalState["verbose"] = True


if __name__ == "__main__":
    app()  # pragma: no cover
