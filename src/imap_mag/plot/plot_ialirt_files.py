import logging
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from imap_mag.io.file import IALiRTQuicklookPathHandler

logger = logging.getLogger(__name__)


def plot_ialirt_files(
    files: list[Path], save_folder: Path
) -> tuple[Path, IALiRTQuicklookPathHandler]:
    """Generate I-ALiRT plots for the specified files."""

    ialirt_data = pd.DataFrame()

    # Load data
    for file in files:
        file_data = pd.read_csv(
            file, parse_dates=["met_in_utc"], index_col="met_in_utc"
        )
        ialirt_data = pd.concat([ialirt_data, file_data])

    if ialirt_data.empty:
        logger.info("No I-ALiRT data available for plotting.")
        return ()  # type: ignore

    min_date: datetime = ialirt_data.index.min().to_pydatetime()
    max_date: datetime = ialirt_data.index.max().to_pydatetime()

    # Plot data
    fig = plt.figure()
    gs = fig.add_gridspec(3, 4)

    # HK dangers
    ax00 = fig.add_subplot(gs[0, 0:2])

    for hk in [
        "mag_hk_hk1v5_danger",
        "mag_hk_hk1v5c_danger",
        "mag_hk_hk1v8_danger",
        "mag_hk_hk1v8c_danger",
        "mag_hk_hk2v5_danger",
        "mag_hk_hk2v5c_danger",
        "mag_hk_hkp8v5_danger",
        "mag_hk_hkp8v5c_danger",
    ]:
        ax00.plot(
            ialirt_data.index,
            ialirt_data[hk],
            label=hk.lstrip("mag_hk_").rstrip("_danger"),
        )

    ax00.set_ylabel("[-]")
    ax00.legend(loc="upper right", fontsize="small", ncol=4)
    ax00.grid()
    ax00.set_title("Danger Limits")

    # 3.3 V
    ax02 = fig.add_subplot(gs[0, 2])

    ax02.plot(ialirt_data.index, ialirt_data["mag_hk_hk3v3"], label="Voltage")
    ax02_left = ax02.twinx()
    ax02_left.plot(
        ialirt_data.index,
        ialirt_data["mag_hk_hk3v3_current"],
        linestyle="--",
        color="orange",
        label="Current",
    )

    ax02.set_ylabel("[V]")
    ax02_left.set_ylabel("[mA]")
    ax02.grid()
    ax02.set_title("3.3 V")

    # -8 V
    ax03 = fig.add_subplot(gs[0, 3])

    ax03.plot(ialirt_data.index, ialirt_data["mag_hk_hkn8v5"], label="Voltage")
    ax03_left = ax03.twinx()
    ax03_left.plot(
        ialirt_data.index,
        ialirt_data["mag_hk_hkn8v5_current"],
        linestyle="--",
        color="orange",
        label="Current",
    )

    ax03.set_ylabel("[V]")
    ax03_left.set_ylabel("[mA]")
    ax03.grid()
    ax03.set_title("-8 V")

    # HK warnings
    ax10 = fig.add_subplot(gs[1, 0:2], sharex=ax00)

    for hk in [
        "mag_hk_hk1v5_warn",
        "mag_hk_hk1v5c_warn",
        "mag_hk_hk1v8_warn",
        "mag_hk_hk1v8c_warn",
        "mag_hk_hk2v5_warn",
        "mag_hk_hk2v5c_warn",
        "mag_hk_hkp8v5_warn",
        "mag_hk_hkp8v5c_warn",
    ]:
        ax10.plot(
            ialirt_data.index,
            ialirt_data[hk],
            label=hk.lstrip("mag_hk_").rstrip("_warn"),
        )

    ax10.set_ylabel("[-]")
    ax10.legend(loc="upper right", fontsize="small", ncol=4)
    ax10.grid()
    ax10.set_title("Warning Limits")

    # Bit errors
    ax12 = fig.add_subplot(gs[1, 2], sharex=ax02)

    ax12.plot(ialirt_data.index, ialirt_data["mag_hk_multbit_errs"])
    ax12.set_ylabel("[-]")
    ax12.grid()
    ax12.set_title("Multibit Errors")

    # Mode
    ax13 = fig.add_subplot(gs[1, 3])

    ax13.plot(ialirt_data.index, ialirt_data["mag_hk_mode"], label="ICU")
    ax13.grid()
    ax13.set_title("Mode")

    # Saturation
    ax20 = fig.add_subplot(gs[2, 0])

    ax20.plot(ialirt_data.index, ialirt_data["mag_hk_fob_saturated"], label="FOB")
    ax20.plot(
        ialirt_data.index,
        ialirt_data["mag_hk_fib_saturated"],
        label="FIB",
        linestyle="--",
    )
    ax20.set_ylabel("[-]")
    ax20.legend(loc="upper right", fontsize="small")
    ax20.grid()
    ax20.set_title("Saturation")

    # Ranges
    ax21 = fig.add_subplot(gs[2, 1])

    ax21.plot(ialirt_data.index, ialirt_data["mag_hk_fob_range"], label="FOB")
    ax21.plot(
        ialirt_data.index, ialirt_data["mag_hk_fib_range"], label="FIB", linestyle="--"
    )
    ax21.set_ylabel("[-]")
    ax21.legend(loc="upper right", fontsize="small")
    ax21.grid()
    ax21.set_title("Ranges")

    # Active
    ax22 = fig.add_subplot(gs[2, 2], sharex=ax02)

    ax22.plot(ialirt_data.index, ialirt_data["mag_hk_pri_isvalid"], label="FOB")
    ax22.plot(
        ialirt_data.index,
        ialirt_data["mag_hk_sec_isvalid"],
        label="FIB",
        linestyle="--",
    )
    ax22.legend(loc="upper right", fontsize="small")
    ax22.grid()
    ax22.set_title("Validity")

    # Temperatures
    ax23 = fig.add_subplot(gs[2, 3], sharex=ax03)

    ax23.plot(ialirt_data.index, ialirt_data["mag_hk_fob_temp"], label="FOB")
    ax23.plot(
        ialirt_data.index, ialirt_data["mag_hk_fib_temp"], label="FIB", linestyle="--"
    )
    ax23.plot(
        ialirt_data.index, ialirt_data["mag_hk_icu_temp"], label="ICU", linestyle=":"
    )
    ax23.set_ylabel("[°C]")
    ax23.legend(loc="upper right", fontsize="small")
    ax23.grid()
    ax23.set_title("Temperatures")

    output_file = save_folder / "ialirt_quicklook.png"

    fig.set_size_inches(20, 12)
    fig.tight_layout()
    fig.savefig(output_file, dpi=100)
    plt.close()

    return output_file, IALiRTQuicklookPathHandler(
        start_date=min_date,
        end_date=max_date,
    )
