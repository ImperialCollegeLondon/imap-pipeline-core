import tempfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from imap_mag.process import HKProcessor, dispatch
from imap_mag.util import HKPacket, TimeConversion
from tests.util.miscellaneous import (
    enableLogging,  # noqa: F401
    tidyDataFolders,  # noqa: F401
)


@pytest.fixture(autouse=False)
def mock_met_to_j2000_conversion_for_hk_power_to_span_two_days(monkeypatch):
    """Mock DatetimeProvider to specific time."""

    original_method = TimeConversion.convert_met_to_j2000ns

    # Add 20 hours (in nanoseconds) to the J2000 result, such that the resulting
    # dataset spans two days (MAG_HSK_PW.pkts is from 2025/05/02 2:18 to 2025/05/02 6:03).
    monkeypatch.setattr(
        TimeConversion,
        "convert_met_to_j2000ns",
        lambda x: original_method(x) + (timedelta(hours=20).seconds * 1e9),
    )


@pytest.mark.parametrize(
    "extension",
    [
        ".pkts",
        ".bin",
    ],
)
def test_dispatch_hk_binary(extension):
    # Set up.
    packet_path = Path("tests/data/2025/MAG_HSK_SOME" + extension)

    # Exercise.
    processor = dispatch(packet_path, Path(tempfile.gettempdir()))

    # Verify.
    assert isinstance(processor, HKProcessor)


def test_dispatch_unsupported_file(caplog):
    # Set up.
    packet_path = Path("tests/data/2025/MAG_HSK_SOME.txt")

    # Exercise and verify.
    with pytest.raises(NotImplementedError) as excinfo:
        dispatch(packet_path, Path(tempfile.gettempdir()))

    assert (
        f"File {packet_path} is not supported and cannot be processed." in caplog.text
    )
    assert f"File {packet_path} is not supported and cannot be processed." in str(
        excinfo.value
    )


@pytest.mark.parametrize(
    "packet_type",
    [
        HKPacket.SID3_PW,
        HKPacket.SID4_STATUS,
        HKPacket.SID5_SCI,
        HKPacket.SID11_PROCSTAT,
        HKPacket.SID15,
    ],
)
def test_decode_hk_packet(packet_type):
    # Set up.
    packet_path = Path("tests/data/2025") / (packet_type.packet + ".pkts")
    expected_path = Path("tests/data/truth") / (packet_type.packet + ".csv")

    processor = HKProcessor(Path(tempfile.gettempdir()))
    processor.initialize(Path("xtce/tlm_20241024.xml"))

    # Exercise.
    processed_paths = processor.process(packet_path)

    # Verify.
    assert len(processed_paths) == 1

    processed_path = processed_paths[0]
    assert processed_path.exists()

    with (
        open(expected_path) as expected_file,
        open(processed_path) as processed_file,
    ):
        expected_lines = expected_file.readlines()
        processed_lines = processed_file.readlines()

        assert processed_lines[0] == expected_lines[0]
        assert processed_lines[1] == expected_lines[1]
        assert processed_lines[-1] == expected_lines[2]
        assert len(processed_lines) == int(expected_lines[3].strip())


def test_decode_hk_packet_with_data_spanning_two_days(
    mock_met_to_j2000_conversion_for_hk_power_to_span_two_days, caplog
):
    """Test that HKProcessor splits data into separate files for each day, for each ApID."""

    # Set up.
    packet_path = Path("tests/data/2025/MAG_HSK_PW.pkts")

    processor = HKProcessor(Path(tempfile.gettempdir()))
    processor.initialize(Path("xtce/tlm_20241024.xml"))

    # Exercise.
    processed_paths = processor.process(packet_path)

    # Verify.
    assert len(processed_paths) == 2

    assert processed_paths[0].exists()
    assert processed_paths[1].exists()

    assert processed_paths[0].name == "imap_mag_hsk-pw_20250502_v000.csv"
    assert processed_paths[1].name == "imap_mag_hsk-pw_20250503_v000.csv"

    df_day1 = pd.read_csv(processed_paths[0], index_col=0)
    epoch_day1 = TimeConversion.convert_j2000ns_to_date(df_day1.index.values)
    assert all([d == date(2025, 5, 2) for d in epoch_day1])

    df_day2 = pd.read_csv(processed_paths[1], index_col=0)
    epoch_day2 = TimeConversion.convert_j2000ns_to_date(df_day2.index.values)
    assert all([d == date(2025, 5, 3) for d in epoch_day2])

    assert (
        "Splitting data for ApID 1063 (MAG_HSK_PW) into separate files for each day:\n20250502, 20250503"
        in caplog.text
    )
    assert "Generating file for 2025-05-02." in caplog.text
    assert "Generating file for 2025-05-03." in caplog.text


def test_decode_hk_packet_with_data_from_multiple_apids(caplog):
    """Test that HKProcessor splits data into separate files for each day, for each ApID."""

    # Set up.
    packet_path = Path(tempfile.gettempdir()) / "MAG_HSK_COMBINED.pkts"

    power_path = Path("tests/data/2025/MAG_HSK_PW.pkts")
    status_path = Path("tests/data/2025/MAG_HSK_STATUS.pkts")

    with open(power_path, "rb") as power_file, open(status_path, "rb") as status_file:
        power_data = power_file.read()
        status_data = status_file.read()

        combined_data = power_data + status_data

    with open(packet_path, "wb") as combined_file:
        combined_file.write(combined_data)

    processor = HKProcessor(Path(tempfile.gettempdir()))
    processor.initialize(Path("xtce/tlm_20241024.xml"))

    # Exercise.
    processed_paths = processor.process(packet_path)

    # Verify.
    assert len(processed_paths) == 2

    assert processed_paths[0].exists()
    assert processed_paths[1].exists()

    assert processed_paths[0].name == "imap_mag_hsk-pw_20250502_v000.csv"
    assert processed_paths[1].name == "imap_mag_hsk-status_20250502_v000.csv"

    assert f"Found 2 ApIDs (1063, 1064) in {packet_path}." in caplog.text
    assert (
        "Splitting data for ApID 1063 (MAG_HSK_PW) into separate files for each day:"
        in caplog.text
    )
    assert (
        "Splitting data for ApID 1064 (MAG_HSK_STATUS) into separate files for each day:"
        in caplog.text
    )


def test_decode_hk_packet_groupby_returns_tuple_for_day():
    """Very specific test to check that we support the `groupby` method returning a tuple for the `day` parameter."""

    # Set up.
    packet_path = Path("tests/data/2025/groupby_day_as_tuple.bin")

    processor = HKProcessor(Path(tempfile.gettempdir()))
    processor.initialize(Path("xtce/tlm_20241024.xml"))

    # Exercise.
    processed_paths = processor.process(packet_path)

    # Verify.
    assert len(processed_paths) == 1

    processed_path = processed_paths[0]
    assert processed_path.exists()

    assert processed_path.name == "imap_mag_hsk-status_20250331_v000.csv"
