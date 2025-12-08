import typer

from imap_mag.cli.fetch.binary import fetch_binary
from imap_mag.cli.fetch.ialirt import fetch_ialirt
from imap_mag.cli.fetch.science import fetch_science
from imap_mag.cli.fetch.spice import fetch_spice, generate_spice_metakernel

app = typer.Typer()

app.command("binary", help="Download binary data from WebPODA")(fetch_binary)
app.command("ialirt", help="Download CSV I-ALiRT data from SDC")(fetch_ialirt)
app.command("science", help="Download CDF science data from SDC")(fetch_science)
app.command("spice", help="Download spice kernels from SDC")(fetch_spice)
app.command("metakernel", help="Build a SPICE metakernel from available kernels")(
    generate_spice_metakernel
)
