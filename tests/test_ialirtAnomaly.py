from datetime import datetime

import pytest

from imap_mag.check import IALiRTAnomaly, SeverityLevel


class CustomAnomaly(IALiRTAnomaly):
    DESCRIPTION = "Some description"

    def get_anomaly_description(self) -> str:
        return self.DESCRIPTION


@pytest.mark.parametrize("severity", [SeverityLevel.Danger, SeverityLevel.Warning])
def test_ialirt_anomaly_logs_description(severity: SeverityLevel, caplog) -> None:
    # Set up.
    anomaly = CustomAnomaly(
        severity=severity,
        time_range=(datetime.now(), datetime.now()),
        parameter="",
        count=0,
    )

    # Exercise.
    anomaly.log()

    # Verify.
    assert f"[{severity.value.upper()}] {anomaly.DESCRIPTION}" in caplog.text
