import typer

from imap_mag.api.fetch.binary import fetch_binary
from imap_mag.api.fetch.science import fetch_science

app = typer.Typer()

app.command("binary", help="Download binary data from WebPODA")(fetch_binary)
app.command("science", help="Download CDF science data from SDC")(fetch_science)
