import logging
from pathlib import Path

import pandas as pd

from imap_mag.check.IALiRTFailure import (
    IALiRTFailure,
    IALiRTFlagFailure,
    IALiRTForbiddenValueFailure,
    IALiRTOutOfBoundsFailure,
    SeverityLevel,
)

logger = logging.getLogger(__name__)


def check_ialirt_files(files: list[Path]) -> list[IALiRTFailure]:
    """Check I-ALiRT data for anomalies."""

    failures: list[IALiRTFailure] = []

    # Load data.
    ialirt_data = pd.DataFrame()

    for file in files:
        file_data: pd.DataFrame = pd.read_csv(
            file, parse_dates=["met_in_utc"], index_col="met_in_utc"
        )
        ialirt_data = pd.concat([ialirt_data, file_data])

    if ialirt_data.empty:
        logger.info("No I-ALiRT data present in files.")
        return failures

    # Set limits and mappings.
    limits_warn_danger: dict[str, tuple[tuple[float, float], tuple[float, float]]] = {
        "mag_hk_hk3v3": ((3.35, 3.38), (3.0, 3.6)),
        "mag_hk_hkn8v5": ((-9.8, -9.46), (-10.61, -8.25)),
    }

    limits_nominal: dict[str, tuple[float, float]] = {
        "mag_hk_hk3v3_current": (110, 130),
        "mag_hk_hkn8v5_current": (80, 100),
    }

    mappings: dict[str, str] = {
        "hk3v3": "3.3 V Voltage",
        "hk3v3_current": "3.3 V Current",
        "hkn8v5": "-8 V Voltage",
        "hkn8v5_current": "-8 V Current",
        "hk1v5": "1.5 V Voltage",
        "hk1v5c": "1.5 V Current",
        "hk1v8": "1.8 V Voltage",
        "hk1v8c": "1.8 V Current",
        "hk2v5": "2.5 V Voltage",
        "hk2v5c": "2.5 V Current",
        "hkp8v5": "+8 V Voltage",
        "hkp8v5c": "+8 V Current",
        "multbit_errs": "Multibit Errors",
        "mode": "Mode",
        "fob_saturated": "FOB Saturated",
        "fib_saturated": "FIB Saturated",
    }

    # Validate 3.3 V and -8 V voltages.
    for col, (
        (min_warn, max_warn),
        (min_danger, max_danger),
    ) in limits_warn_danger.items():
        if col not in ialirt_data.columns:
            continue

        danger_failure: IALiRTFailure | None = check_data_is_between_limits(
            ialirt_data,
            col,
            min_danger,
            max_danger,
            SeverityLevel.Danger,
            mappings[col],
        )

        if danger_failure:
            failures.append(danger_failure)
            continue

        warning_failure: IALiRTFailure | None = check_data_is_between_limits(
            ialirt_data,
            col,
            min_warn,
            max_warn,
            SeverityLevel.Warning,
            mappings[col],
        )

        if warning_failure:
            failures.append(warning_failure)

    # Validate 3.3 V and -8 V currents.
    for col, (min_nominal, max_nominal) in limits_nominal.items():
        if col not in ialirt_data.columns:
            continue

        warning_failure = check_data_is_between_limits(
            ialirt_data,
            col,
            min_nominal,
            max_nominal,
            SeverityLevel.Warning,
            mappings[col],
        )

        if warning_failure:
            failures.append(warning_failure)

    # Validate other voltages and currents.
    for col in ialirt_data.columns:
        if col.endswith("_danger"):
            danger_failure = check_data_is_false(
                ialirt_data,
                col,
                SeverityLevel.Danger,
                mappings[col.lstrip("mag_hk_").rstrip("_danger")],
            )

            if danger_failure:
                failures.append(danger_failure)
                continue

        if col.endswith("_warn"):
            warning_failure = check_data_is_false(
                ialirt_data,
                col,
                SeverityLevel.Warning,
                mappings[col.lstrip("mag_hk_").rstrip("_warn")],
            )

            if warning_failure:
                failures.append(warning_failure)

    # Mode.
    if "mag_hk_mode" in ialirt_data.columns:
        invalid_data = ialirt_data[
            ialirt_data["mag_hk_mode"].notna() & ialirt_data["mag_hk_mode"].ne(2)
        ]

        if not invalid_data.empty:
            failures.append(
                IALiRTForbiddenValueFailure(
                    time_range=(
                        invalid_data.index.min().to_pydatetime(),
                        invalid_data.index.max().to_pydatetime(),
                    ),
                    value="Safe",
                    parameter="Mode",
                    severity=SeverityLevel.Warning,
                )
            )

    # Multibit errors.
    if "mag_hk_multbit_errs" in ialirt_data.columns:
        multibit_failure = check_data_is_between_limits(
            ialirt_data,
            "mag_hk_multbit_errs",
            0,
            0,
            SeverityLevel.Danger,
            mappings["multbit_errs"],
        )

        if multibit_failure:
            failures.append(multibit_failure)

    # Saturation flags.
    for col in ["mag_hk_fob_saturated", "mag_hk_fib_saturated"]:
        if col not in ialirt_data.columns:
            continue

        warning_failure = check_data_is_false(
            ialirt_data,
            col,
            SeverityLevel.Warning,
            mappings[col.lstrip("mag_hk_")],
        )

        if warning_failure:
            failures.append(warning_failure)

    return failures


def check_data_is_between_limits(
    ialirt_data: pd.DataFrame,
    col: str,
    min_value: float,
    max_value: float,
    severity: SeverityLevel,
    pretty_name: str,
) -> IALiRTFailure | None:
    out_of_bounds_data = ialirt_data[
        ialirt_data[col].notna()
        & (ialirt_data[col].lt(min_value) | ialirt_data[col].gt(max_value))
    ]

    if not out_of_bounds_data.empty:
        return IALiRTOutOfBoundsFailure(
            time_range=(
                out_of_bounds_data.index.min().to_pydatetime(),
                out_of_bounds_data.index.max().to_pydatetime(),
            ),
            parameter=pretty_name,
            severity=severity,
            values=(out_of_bounds_data[col].min(), out_of_bounds_data[col].max()),
            limits=(min_value, max_value),
        )

    return None


def check_data_is_false(
    ialirt_data: pd.DataFrame,
    col: str,
    severity: SeverityLevel,
    pretty_name: str,
) -> IALiRTFailure | None:
    true_data = ialirt_data[ialirt_data[col].notna() & ialirt_data[col]]

    if not true_data.empty:
        return IALiRTFlagFailure(
            time_range=(
                true_data.index.min().to_pydatetime(),
                true_data.index.max().to_pydatetime(),
            ),
            parameter=pretty_name,
            severity=severity,
        )

    return None
