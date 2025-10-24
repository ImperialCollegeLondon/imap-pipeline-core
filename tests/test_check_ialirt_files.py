from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
import yaml

from imap_mag.check import (
    IALiRTAnomaly,
    IALiRTFlagAnomaly,
    IALiRTForbiddenValueAnomaly,
    IALiRTOutOfBoundsAnomaly,
    SeverityLevel,
    check_ialirt_files,
)
from imap_mag.util.constants import CONSTANTS


def write_test_ialirt_packet_definition_file(folder: Path) -> Path:
    """Write a test I-ALiRT packet definition file to the given folder."""

    file: Path = folder / CONSTANTS.IALIRT_PACKET_DEFINITION_FILE
    content: dict = {
        "ialirt_human_readable_names": [
            {"danger_limit_param": "My Danger Limit"},
            {"warning_limit_param": "My Warning Limit"},
            {"both_limit_param": "My Limit"},
            {"forbidden_param": "My Forbidden"},
            {"forbidden_lookup_param": "My Forbidden Lookup"},
            {"flag_param": "My Flag"},
        ],
        "ialirt_validation": [
            {
                "name": "danger_limit_param",
                "type": "limit",
                "danger_min": 10,
                "danger_max": 20,
            },
            {
                "name": "warning_limit_param",
                "type": "limit",
                "warning_min": 20,
                "warning_max": 30,
            },
            {
                "name": "both_limit_param",
                "type": "limit",
                "danger_min": 30,
                "danger_max": 50,
                "warning_min": 35,
                "warning_max": 45,
            },
            {
                "name": "forbidden_param",
                "type": "forbidden",
                "values": [99, 100],
                "severity": "warning",
            },
            {
                "name": "forbidden_lookup_param",
                "type": "forbidden",
                "values": [101, 102],
                "severity": "danger",
                "lookup": {
                    101: "Bad 101",
                    102: "Bad 102",
                },
            },
            {
                "name": "flag_param",
                "type": "flag",
                "severity": "danger",
            },
        ],
    }

    file.write_text(yaml.dump(content))

    return file


def write_test_ialirt_data_file(folder: Path) -> Path:
    file: Path = folder / "ialirt_data.csv"
    file.write_text(
        "met_in_utc,danger_limit_param,warning_limit_param,both_limit_param,forbidden_param,forbidden_lookup_param,flag_param\n"
        "2024-01-01T00:00:00,15,25,40,50,50,0\n"
        "2024-01-01T01:00:00,14,26,41,60,60,0\n"
        "2024-01-01T02:00:00,13,27,42,70,70,0\n"
    )

    return file


def test_check_ialirt_empty_files(temp_folder_path, caplog) -> None:
    # Set up.
    write_test_ialirt_packet_definition_file(temp_folder_path)

    # Execute.
    anomalies: list[IALiRTAnomaly] = check_ialirt_files(
        files=[],
        packet_definition_folder=temp_folder_path,
    )

    # Verify.
    assert len(anomalies) == 0
    assert "No I-ALiRT data present in files." in caplog.text


def test_check_ialirt_files_no_anomalies(temp_folder_path) -> None:
    # Set up.
    write_test_ialirt_packet_definition_file(temp_folder_path)
    test_ialirt_data = write_test_ialirt_data_file(temp_folder_path)

    # Execute.
    anomalies: list[IALiRTAnomaly] = check_ialirt_files(
        files=[test_ialirt_data],
        packet_definition_folder=temp_folder_path,
    )

    # Verify.
    assert len(anomalies) == 0


@pytest.mark.parametrize(
    "fail_param,fail_value,expected_anomaly",
    [
        (
            "danger_limit_param",
            25,
            IALiRTOutOfBoundsAnomaly(
                time_range=(datetime(2024, 1, 1, 1), datetime(2024, 1, 1, 1)),
                parameter="My Danger Limit",
                severity=SeverityLevel.Danger,
                count=1,
                value=25.0,
                limits=(10, 20),
            ),
        ),
        (
            "warning_limit_param",
            35,
            IALiRTOutOfBoundsAnomaly(
                time_range=(datetime(2024, 1, 1, 1), datetime(2024, 1, 1, 1)),
                parameter="My Warning Limit",
                severity=SeverityLevel.Warning,
                count=1,
                value=35.0,
                limits=(20, 30),
            ),
        ),
        (
            "both_limit_param",
            46,
            IALiRTOutOfBoundsAnomaly(
                time_range=(datetime(2024, 1, 1, 1), datetime(2024, 1, 1, 1)),
                parameter="My Limit",
                severity=SeverityLevel.Warning,
                count=1,
                value=46.0,
                limits=(35, 45),
            ),
        ),
        (
            "both_limit_param",
            29,
            IALiRTOutOfBoundsAnomaly(
                time_range=(datetime(2024, 1, 1, 1), datetime(2024, 1, 1, 1)),
                parameter="My Limit",
                severity=SeverityLevel.Danger,
                count=1,
                value=29.0,
                limits=(30, 50),
            ),
        ),
        (
            "forbidden_param",
            99,
            IALiRTForbiddenValueAnomaly(
                time_range=(datetime(2024, 1, 1, 1), datetime(2024, 1, 1, 1)),
                parameter="My Forbidden",
                severity=SeverityLevel.Warning,
                count=1,
                value=99.0,
            ),
        ),
        (
            "forbidden_lookup_param",
            101,
            IALiRTForbiddenValueAnomaly(
                time_range=(datetime(2024, 1, 1, 1), datetime(2024, 1, 1, 1)),
                parameter="My Forbidden Lookup",
                severity=SeverityLevel.Danger,
                count=1,
                value="Bad 101",
            ),
        ),
        (
            "flag_param",
            1,
            IALiRTFlagAnomaly(
                time_range=(datetime(2024, 1, 1, 1), datetime(2024, 1, 1, 1)),
                parameter="My Flag",
                severity=SeverityLevel.Danger,
                count=1,
            ),
        ),
    ],
)
def test_check_ialirt_files_with_anomalies(
    temp_folder_path, fail_param, fail_value, expected_anomaly
) -> None:
    # Set up.
    write_test_ialirt_packet_definition_file(temp_folder_path)
    test_ialirt_data = write_test_ialirt_data_file(temp_folder_path)

    ialirt_data = pd.read_csv(test_ialirt_data)
    ialirt_data.loc[1, fail_param] = fail_value  # introduce anomaly
    ialirt_data.to_csv(test_ialirt_data, index=False)

    # Execute.
    anomalies: list[IALiRTAnomaly] = check_ialirt_files(
        files=[test_ialirt_data],
        packet_definition_folder=temp_folder_path,
    )

    # Verify.
    assert len(anomalies) == 1
    assert anomalies[0] == expected_anomaly
