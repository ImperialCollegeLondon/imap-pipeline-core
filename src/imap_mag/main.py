"""Main module."""

from typing import Annotated

import typer

from imap_mag.cli import calibrate, process, publish
from imap_mag.cli.check import check
from imap_mag.cli.cliUtils import globalState
from imap_mag.cli.fetch import fetch
from imap_mag.cli.plot import plot

app = typer.Typer()


@app.command()
def hello(name: str):
    print(f"Hello {name}")


app.command()(process.process)
app.command()(calibrate.calibrate)
app.command()(publish.publish)

app.add_typer(fetch.app, name="fetch", help="Fetch data from the SDC or WebPODA")
app.add_typer(calibrate.app, name="calibration", help="Generate calibration parameters")
app.add_typer(check.app, name="check", help="Check data from the datastore")
app.add_typer(plot.app, name="plot", help="Plot data from the datastore")


@app.callback()
def main(verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False):
    if verbose:
        globalState["verbose"] = True


if __name__ == "__main__":
    app()  # pragma: no cover
