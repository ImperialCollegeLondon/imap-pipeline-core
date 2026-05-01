import typer

from imap_mag.cli.fetch.binary import fetch_binary
from imap_mag.cli.fetch.ialirt import fetch_ialirt, fetch_ialirt_hk
from imap_mag.cli.fetch.science import fetch_science
from imap_mag.cli.fetch.spice import fetch_spice, generate_spice_metakernel
from imap_mag.cli.fetch.spin_table import fetch_spin_tables
from imap_mag.cli.fetch.webtcad import (
    fetch_hi45_esa_step,
    fetch_hi90_esa_step,
    fetch_lo_pivot_platform_angle,
)

app = typer.Typer()

app.command("binary", help="Download binary data from WebPODA")(fetch_binary)
app.command("ialirt", help="Download CSV I-ALiRT MAG data from SDC")(fetch_ialirt)
app.command("ialirt-hk", help="Download CSV I-ALiRT MAG HK data from SDC")(
    fetch_ialirt_hk
)
app.command("science", help="Download CDF science data from SDC")(fetch_science)
app.command("spice", help="Download spice kernels from SDC")(fetch_spice)
app.command("metakernel", help="Build a SPICE metakernel from available kernels")(
    generate_spice_metakernel
)
app.command("spin-tables", help="Download spin table files from SDC")(fetch_spin_tables)
app.command(
    "imap-lo-pivot-platform",
    help="Download IMAP-Lo pivot platform angle HK CSV data from WebTCAD LaTiS",
)(fetch_lo_pivot_platform_angle)
app.command(
    "imap-hi45-step",
    help="Download IMAP-Hi 45 ESA STEP HK CSV data from WebTCAD LaTiS",
)(fetch_hi45_esa_step)
app.command(
    "imap-hi90-step",
    help="Download IMAP-Hi 90 ESA STEP HK CSV data from WebTCAD LaTiS",
)(fetch_hi90_esa_step)
