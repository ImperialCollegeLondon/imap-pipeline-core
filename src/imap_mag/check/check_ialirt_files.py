import logging
from pathlib import Path

import pandas as pd
import yaml

from imap_mag.check.IALiRTAnomaly import (
    IALiRTAnomaly,
    IALiRTFlagAnomaly,
    IALiRTForbiddenValueAnomaly,
    IALiRTOutOfBoundsAnomaly,
)
from imap_mag.check.SeverityLevel import SeverityLevel
from imap_mag.process import get_packet_definition_folder
from imap_mag.util.constants import CONSTANTS

logger = logging.getLogger(__name__)


def check_ialirt_files(
    files: list[Path], packet_definition_folder: Path
) -> list[IALiRTAnomaly]:
    """Check I-ALiRT data for anomalies."""

    anomalies: list[IALiRTAnomaly] = []

    # Load data.
    ialirt_data = pd.DataFrame()

    for file in files:
        file_data: pd.DataFrame = pd.read_csv(
            file, parse_dates=["met_in_utc"], index_col="met_in_utc"
        )
        ialirt_data = pd.concat([ialirt_data, file_data])

    if ialirt_data.empty:
        logger.info("No I-ALiRT data present in files.")
        return anomalies

    # Load packet definition
    packet_definition_file: Path = (
        get_packet_definition_folder(packet_definition_folder)
        / CONSTANTS.IALIRT_PACKET_DEFINITION_FILE
    )
    packet_definition: dict = yaml.safe_load(packet_definition_file.read_text())

    human_readabale_names: list[dict] = packet_definition["ialirt_human_readable_names"]
    mappings: dict[str, str] = {
        k: v for d in human_readabale_names for k, v in d.items()
    }
    validation: dict = packet_definition["ialirt_validation"]

    # Check parameters according to validation rules
    for parameter in validation:
        name = parameter["name"]
        type = parameter["type"]

        if name not in ialirt_data.columns:
            logger.warning(f"Parameter {name} not found in I-ALiRT data columns.")
            continue

        match type:
            case "limit":  # ------- Check for out-of-bounds values -------
                danger_min = parameter.get("danger_min", None)
                danger_max = parameter.get("danger_max", None)
                warn_min = parameter.get("warning_min", None)
                warn_max = parameter.get("warning_max", None)

                if danger_min and danger_max:
                    danger_anomalies: list[IALiRTAnomaly] = (
                        check_data_is_between_limits(
                            ialirt_data,
                            name,
                            danger_min,
                            danger_max,
                            SeverityLevel.Danger,
                            mappings[name],
                        )
                    )

                    if danger_anomalies:
                        anomalies.extend(danger_anomalies)
                        continue  # skip warning check if danger found

                if warn_min and warn_max:
                    anomalies.extend(
                        check_data_is_between_limits(
                            ialirt_data,
                            name,
                            warn_min,
                            warn_max,
                            SeverityLevel.Warning,
                            mappings[name],
                        )
                    )

            case "forbidden":  # ------- Check for forbidden values -------
                forbidden_values = parameter.get("values", [])
                severity = parameter.get("severity", "danger")
                lookup = parameter.get("lookup", None)

                anomalies.extend(
                    check_data_not_equal_to(
                        ialirt_data,
                        name,
                        forbidden_values,
                        SeverityLevel(severity),
                        mappings[name],
                        lookup,
                    )
                )

            case "flag":  # ------- Check for flag values -------
                severity = parameter.get("severity", "danger")

                flag_anomaly: IALiRTAnomaly | None = check_data_is_false(
                    ialirt_data,
                    name,
                    SeverityLevel(severity),
                    mappings[name.removesuffix("_warn").removesuffix("_danger")],
                )

                if flag_anomaly:
                    anomalies.append(flag_anomaly)

            case _:  # ------- Unknown check -------
                logger.error(f"Unknown validation type {type} for parameter {name}.")

    return anomalies


def check_data_is_between_limits(
    ialirt_data: pd.DataFrame,
    name: str,
    min_value: float,
    max_value: float,
    severity: SeverityLevel,
    pretty_name: str,
) -> list[IALiRTAnomaly]:
    out_of_bounds_anomalies: list[IALiRTAnomaly] = []

    out_of_upper_bound_data = ialirt_data[
        ialirt_data[name].notna() & ialirt_data[name].gt(max_value)
    ]

    if not out_of_upper_bound_data.empty:
        out_of_bounds_anomalies.append(
            IALiRTOutOfBoundsAnomaly(
                time_range=(
                    out_of_upper_bound_data.index.min().to_pydatetime(),
                    out_of_upper_bound_data.index.max().to_pydatetime(),
                ),
                parameter=pretty_name,
                severity=severity,
                count=len(out_of_upper_bound_data),
                value=float(out_of_upper_bound_data[name].max()),
                limits=(min_value, max_value),
            )
        )

    out_of_lower_bound_data = ialirt_data[
        ialirt_data[name].notna() & ialirt_data[name].lt(min_value)
    ]

    if not out_of_lower_bound_data.empty:
        out_of_bounds_anomalies.append(
            IALiRTOutOfBoundsAnomaly(
                time_range=(
                    out_of_lower_bound_data.index.min().to_pydatetime(),
                    out_of_lower_bound_data.index.max().to_pydatetime(),
                ),
                parameter=pretty_name,
                severity=severity,
                count=len(out_of_lower_bound_data),
                value=float(out_of_lower_bound_data[name].max()),
                limits=(min_value, max_value),
            )
        )

    return out_of_bounds_anomalies


def check_data_not_equal_to(
    ialirt_data: pd.DataFrame,
    name: str,
    values: list[float | str],
    severity: SeverityLevel,
    pretty_name: str,
    lookup: dict[float | str, str] | None = None,
) -> list[IALiRTAnomaly]:
    forbidden_anomalies: list[IALiRTAnomaly] = []

    for v in values:
        invalid_data = ialirt_data[ialirt_data[name].notna() & ialirt_data[name].eq(v)]

        if not invalid_data.empty:
            forbidden_anomalies.append(
                IALiRTForbiddenValueAnomaly(
                    time_range=(
                        invalid_data.index.min().to_pydatetime(),
                        invalid_data.index.max().to_pydatetime(),
                    ),
                    value=lookup[v] if lookup else v,
                    parameter=pretty_name,
                    severity=severity,
                    count=len(invalid_data),
                )
            )

    return forbidden_anomalies


def check_data_is_false(
    ialirt_data: pd.DataFrame,
    name: str,
    severity: SeverityLevel,
    pretty_name: str,
) -> IALiRTAnomaly | None:
    true_data = ialirt_data[ialirt_data[name].notna() & ialirt_data[name]]

    if not true_data.empty:
        return IALiRTFlagAnomaly(
            time_range=(
                true_data.index.min().to_pydatetime(),
                true_data.index.max().to_pydatetime(),
            ),
            parameter=pretty_name,
            severity=severity,
            count=len(true_data),
        )

    return None
