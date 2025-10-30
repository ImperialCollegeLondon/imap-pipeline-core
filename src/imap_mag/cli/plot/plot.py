import typer

from imap_mag.cli.plot.plot_ialirt import plot_ialirt

app = typer.Typer()

app.command("ialirt", help="Plot I-ALiRT data")(plot_ialirt)
