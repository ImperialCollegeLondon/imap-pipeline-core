import logging
from datetime import datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import FuncFormatter

from imap_mag.db import Database
from imap_mag.io.file import IALiRTQuicklookPathHandler
from imap_mag.util import DatetimeProvider
from imap_mag.util.constants import CONSTANTS

logger = logging.getLogger(__name__)


def plot_ialirt_files(
    files: list[Path], save_folder: Path, combine_plots: bool = False
) -> dict[Path, IALiRTQuicklookPathHandler]:
    """Generate I-ALiRT plots for the specified files."""

    generated_files: dict[Path, IALiRTQuicklookPathHandler] = {}

    if combine_plots:
        # Combine I-ALiRT data from all files into a single plot
        ialirt_data = pd.DataFrame()

        for file in files:
            file_data: pd.DataFrame = pd.read_csv(
                file, parse_dates=["met_in_utc"], index_col="met_in_utc"
            )
            ialirt_data = pd.concat([ialirt_data, file_data])

        if ialirt_data.empty:
            logger.info("No I-ALiRT data present in files.")
            return generated_files

        (output_file, output_handler) = create_figure(ialirt_data, save_folder)
        generated_files[output_file] = output_handler
    else:
        # Generate individual plots for each I-ALiRT file
        for file in files:
            ialirt_data: pd.DataFrame = pd.read_csv(
                file, parse_dates=["met_in_utc"], index_col="met_in_utc"
            )

            if ialirt_data.empty:
                logger.info(f"No I-ALiRT data available in {file}.")
                continue

            (output_file, output_handler) = create_figure(ialirt_data, save_folder)
            generated_files[output_file] = output_handler

    return generated_files


def create_figure(
    ialirt_data: pd.DataFrame, save_folder: Path
) -> tuple[Path, IALiRTQuicklookPathHandler]:
    fig = plt.figure()
    gs = fig.add_gridspec(3, 4)

    # GSE science
    ax00 = fig.add_subplot(gs[0, 0:2])

    for i, b in enumerate(
        [
            "mag_B_GSE_x",
            "mag_B_GSE_y",
            "mag_B_GSE_z",
            "mag_B_magnitude",
        ]
    ):
        ax00.plot(
            ialirt_data[ialirt_data[b].notna()].index,
            ialirt_data[ialirt_data[b].notna()][b] * (1 + i * 0.1),
            label=b.lstrip("mag_B_GSE_") if b != "mag_B_magnitude" else "|B|",
        )

    ax00.set_ylabel("[nT]")
    ax00.legend(loc="upper right", fontsize="small", ncol=4)
    ax00.grid()
    ax00.set_title("GSE Field")

    # HK dangers
    ax02 = fig.add_subplot(gs[0, 2])

    for i, hk in enumerate(
        [
            "mag_hk_hk1v5_danger",
            "mag_hk_hk1v5c_danger",
            "mag_hk_hk1v8_danger",
            "mag_hk_hk1v8c_danger",
            "mag_hk_hk2v5_danger",
            "mag_hk_hk2v5c_danger",
            "mag_hk_hkp8v5_danger",
            "mag_hk_hkp8v5c_danger",
        ]
    ):
        ax02.plot(
            ialirt_data[ialirt_data[hk].notna()].index,
            ialirt_data[ialirt_data[hk].notna()][hk] * (1 + i * 0.1),
            label=hk.lstrip("mag_hk_").rstrip("_danger"),
        )

    ax02.set_ylabel("0 == OK")
    ax02.legend(loc="upper right", fontsize="small", ncol=4)
    ax02.grid()
    ax02.set_title("Danger Limits")

    # HK warnings
    ax03 = fig.add_subplot(gs[0, 3])

    for i, hk in enumerate(
        [
            "mag_hk_hk1v5_warn",
            "mag_hk_hk1v5c_warn",
            "mag_hk_hk1v8_warn",
            "mag_hk_hk1v8c_warn",
            "mag_hk_hk2v5_warn",
            "mag_hk_hk2v5c_warn",
            "mag_hk_hkp8v5_warn",
            "mag_hk_hkp8v5c_warn",
        ]
    ):
        ax03.plot(
            ialirt_data[ialirt_data[hk].notna()].index,
            ialirt_data[ialirt_data[hk].notna()][hk] * (1 + i * 0.1),
            label=hk.lstrip("mag_hk_").rstrip("_warn"),
        )

    ax03.set_ylabel("0 == OK")
    ax03.legend(loc="upper right", fontsize="small", ncol=4)
    ax03.grid()
    ax03.set_title("Warning Limits")

    # 3.3 V
    ax10 = fig.add_subplot(gs[1, 0])

    ax10.plot(
        ialirt_data[ialirt_data["mag_hk_hk3v3"].notna()].index,
        ialirt_data[ialirt_data["mag_hk_hk3v3"].notna()]["mag_hk_hk3v3"],
        linestyle="--",
        label="Voltage",
    )
    ax10_left = ax10.twinx()
    ax10_left.plot(
        ialirt_data[ialirt_data["mag_hk_hk3v3_current"].notna()].index,
        ialirt_data[ialirt_data["mag_hk_hk3v3_current"].notna()][
            "mag_hk_hk3v3_current"
        ],
        color="orange",
        label="Current",
    )

    ax10.set_ylabel("[V]")
    ax10_left.set_ylabel("[mA]")
    ax10.grid()
    ax10.set_title("3.3 V")

    # -8 V
    ax11 = fig.add_subplot(gs[1, 1])

    ax11.plot(
        ialirt_data[ialirt_data["mag_hk_hkn8v5"].notna()].index,
        ialirt_data[ialirt_data["mag_hk_hkn8v5"].notna()]["mag_hk_hkn8v5"],
        linestyle="--",
        label="Voltage",
    )
    ax11_left = ax11.twinx()
    ax11_left.plot(
        ialirt_data[ialirt_data["mag_hk_hkn8v5_current"].notna()].index,
        ialirt_data[ialirt_data["mag_hk_hkn8v5_current"].notna()][
            "mag_hk_hkn8v5_current"
        ],
        color="orange",
        label="Current",
    )

    ax11.set_ylabel("[V]")
    ax11_left.set_ylabel("[mA]")
    ax11.grid()
    ax11.set_title("-8 V")

    # Bit errors
    ax12 = fig.add_subplot(gs[1, 2])

    ax12.plot(
        ialirt_data[ialirt_data["mag_hk_multbit_errs"].notna()].index,
        ialirt_data[ialirt_data["mag_hk_multbit_errs"].notna()]["mag_hk_multbit_errs"],
    )
    ax12.set_ylabel("[-]")
    ax12.grid()
    ax12.set_title("Multibit Errors")

    # Mode
    ax13 = fig.add_subplot(gs[1, 3])

    ax13.plot(
        ialirt_data[ialirt_data["mag_hk_mode"].notna()].index,
        ialirt_data[ialirt_data["mag_hk_mode"].notna()]["mag_hk_mode"],
        label="ICU",
    )
    ax13.grid()
    ax13.set_title("Mode")

    # Saturation
    ax20 = fig.add_subplot(gs[2, 0])

    ax20.plot(
        ialirt_data[ialirt_data["mag_hk_fob_saturated"].notna()].index,
        ialirt_data[ialirt_data["mag_hk_fob_saturated"].notna()][
            "mag_hk_fob_saturated"
        ],
        label="FOB",
    )
    ax20.plot(
        ialirt_data[ialirt_data["mag_hk_fib_saturated"].notna()].index,
        ialirt_data[ialirt_data["mag_hk_fib_saturated"].notna()][
            "mag_hk_fib_saturated"
        ],
        label="FIB",
        linestyle="--",
    )
    ax20.set_ylabel("[-]")
    ax20.legend(loc="upper right", fontsize="small")
    ax20.grid()
    ax20.set_title("Saturation")

    # Ranges
    ax21 = fig.add_subplot(gs[2, 1])

    ax21.plot(
        ialirt_data[ialirt_data["mag_hk_fob_range"].notna()].index,
        ialirt_data[ialirt_data["mag_hk_fob_range"].notna()]["mag_hk_fob_range"],
        label="FOB",
    )
    ax21.plot(
        ialirt_data[ialirt_data["mag_hk_fib_range"].notna()].index,
        ialirt_data[ialirt_data["mag_hk_fib_range"].notna()]["mag_hk_fib_range"],
        label="FIB",
        linestyle="--",
    )
    ax21.set_ylabel("[-]")
    ax21.legend(loc="upper right", fontsize="small")
    ax21.grid()
    ax21.set_title("Ranges")

    # Active
    ax22 = fig.add_subplot(gs[2, 2])

    ax22.plot(
        ialirt_data[ialirt_data["mag_hk_pri_isvalid"].notna()].index,
        ialirt_data[ialirt_data["mag_hk_pri_isvalid"].notna()]["mag_hk_pri_isvalid"],
        label="FOB",
    )
    ax22.plot(
        ialirt_data[ialirt_data["mag_hk_sec_isvalid"].notna()].index,
        ialirt_data[ialirt_data["mag_hk_sec_isvalid"].notna()]["mag_hk_sec_isvalid"],
        label="FIB",
        linestyle="--",
    )
    ax22.legend(loc="upper right", fontsize="small")
    ax22.grid()
    ax22.set_title("Status")

    # Temperatures
    ax23 = fig.add_subplot(gs[2, 3])

    ax23.plot(
        ialirt_data[ialirt_data["mag_hk_fob_temp"].notna()].index,
        ialirt_data[ialirt_data["mag_hk_fob_temp"].notna()]["mag_hk_fob_temp"],
        label="FOB",
    )
    ax23.plot(
        ialirt_data[ialirt_data["mag_hk_fib_temp"].notna()].index,
        ialirt_data[ialirt_data["mag_hk_fib_temp"].notna()]["mag_hk_fib_temp"],
        label="FIB",
        linestyle="--",
    )
    ax23.plot(
        ialirt_data[ialirt_data["mag_hk_icu_temp"].notna()].index,
        ialirt_data[ialirt_data["mag_hk_icu_temp"].notna()]["mag_hk_icu_temp"],
        label="ICU",
        linestyle=":",
    )
    ax23.set_ylabel("[°C]")
    ax23.legend(loc="upper right", fontsize="small")
    ax23.grid()
    ax23.set_title("Temperatures")

    # Save figure
    max_date: datetime = ialirt_data.index.max().to_pydatetime()
    output_file = save_folder / f"ialirt_quicklook_{max_date.strftime('%Y%m%d')}.png"

    set_time_format(fig)
    set_figure_title(fig)

    fig.set_size_inches(22, 12)
    fig.tight_layout()
    fig.savefig(output_file, dpi=100)
    plt.close()

    return (
        output_file,
        IALiRTQuicklookPathHandler(
            content_date=max_date,
        ),
    )


def set_time_format(fig: plt.Figure) -> None:
    for ax in fig.get_axes():
        x_lim = ax.get_xlim()
        time_span_hours = (x_lim[1] - x_lim[0]) * 24

        # Show major ticks every 15, 30, or 60 minutes
        if time_span_hours < 1:
            ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=15))
        elif time_span_hours < 3:
            ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=30))
        elif time_span_hours < 12:
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
        elif time_span_hours < 36:
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
        else:
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=4))

        def format_time_with_date(x, pos):
            date = mdates.num2date(x)
            if (date.hour <= 3) or (pos == 0):
                return date.strftime("%d/%m %H:%M")
            else:
                return date.strftime("%H:%M")

        # Style major ticks
        ax.xaxis.set_major_formatter(FuncFormatter(format_time_with_date))
        ax.tick_params(which="major", labelsize=8, length=5)
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")


def set_figure_title(fig: plt.Figure) -> None:
    database = Database()
    time_format = "%Y-%m-%d %H:%M:%S"

    latest_data_timestamp = database.get_workflow_progress(
        CONSTANTS.DATABASE.IALIRT_PROGRESS_ID
    ).get_progress_timestamp()
    latest_check_timestamp = database.get_workflow_progress(
        CONSTANTS.DATABASE.IALIRT_VALIDATION_ID
    ).get_last_checked_date()

    fig.suptitle(
        "I-ALiRT Quicklook\n"
        f"Generated at: {DatetimeProvider.now().strftime(time_format)} (UTC)\n"
        f"Last downloaded timestamp: {latest_data_timestamp.strftime(time_format) if latest_data_timestamp else 'N/A'} (UTC)\n"
        f"Last check run at: {latest_check_timestamp.strftime(time_format) if latest_check_timestamp else 'N/A'} (UTC)",
        fontsize=14,
    )
