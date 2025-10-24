import typer

from imap_mag.cli.check.check_ialirt import check_ialirt

app = typer.Typer()

app.command("ialirt", help="Check I-ALiRT HK for anomalies")(check_ialirt)
