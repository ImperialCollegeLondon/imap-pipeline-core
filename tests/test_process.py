import tempfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from imap_mag.io import HKPathHandler, IFilePathHandler, InputManager
from imap_mag.process import HKProcessor, dispatch
from imap_mag.util import HKPacket, TimeConversion
from tests.util.miscellaneous import DATASTORE, tidyDataFolders  # noqa: F401


def instantiate_hk_processor():
    """Instantiate HKProcessor with a temporary work folder."""
    work_folder = Path(tempfile.gettempdir())
    return HKProcessor(work_folder, InputManager(DATASTORE))


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
    processor = dispatch(
        packet_path,
        Path(tempfile.gettempdir()),
        InputManager(DATASTORE),
    )

    # Verify.
    assert isinstance(processor, HKProcessor)


def test_dispatch_unsupported_file(capture_cli_logs):
    # Set up.
    packet_path = Path("tests/data/2025/MAG_HSK_SOME.txt")

    # Exercise and verify.
    with pytest.raises(
        NotImplementedError,
        match=f"File {packet_path} is not supported and cannot be processed.",
    ):
        dispatch(
            packet_path,
            Path(tempfile.gettempdir()),
            InputManager(DATASTORE),
        )

    assert (
        f"File {packet_path} is not supported and cannot be processed."
        in capture_cli_logs.text
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

    processor = instantiate_hk_processor()
    processor.initialize(Path("xtce/tlm_20241024.xml"))

    # Exercise.
    processed_files: dict[Path, IFilePathHandler] = processor.process(packet_path)

    # Verify.
    assert len(processed_files) == 1

    processed_path: Path = next(iter(processed_files))
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

    processed_handler: IFilePathHandler = processed_files[processed_path]

    assert isinstance(processed_handler, HKPathHandler)

    assert processed_handler.level == "l1"
    assert processed_handler.descriptor == (
        packet_type.packet.lower().strip("mag_").replace("_", "-")
    )


def test_decode_hk_packet_with_data_spanning_two_days(
    mock_met_to_j2000_conversion_for_hk_power_to_span_two_days, capture_cli_logs
):
    """Test that HKProcessor splits data into separate files for each day, for each ApID."""

    # Set up.
    packet_path = Path("tests/data/2025/MAG_HSK_PW.pkts")

    processor = instantiate_hk_processor()
    processor.initialize(Path("xtce/tlm_20241024.xml"))

    # Exercise.
    processed_files: dict[Path, IFilePathHandler] = processor.process(packet_path)

    # Verify.
    assert len(processed_files) == 2

    processed_path1, processed_path2 = processed_files.keys()

    assert processed_path1.exists()
    assert processed_path2.exists()

    assert processed_path2.name == "imap_mag_l1_hsk-pw_20250503_v001.csv"
    assert processed_path1.name == "imap_mag_l1_hsk-pw_20250502_v001.csv"

    df_day1 = pd.read_csv(processed_path1, index_col=0)
    epoch_day1 = TimeConversion.convert_j2000ns_to_date(df_day1.index.values)
    assert all([d == date(2025, 5, 2) for d in epoch_day1])

    df_day2 = pd.read_csv(processed_path2, index_col=0)
    epoch_day2 = TimeConversion.convert_j2000ns_to_date(df_day2.index.values)
    assert all([d == date(2025, 5, 3) for d in epoch_day2])

    assert (
        "Splitting data for ApID 1063 (MAG_HSK_PW) into separate files for each day:\n20250502, 20250503"
        in capture_cli_logs.text
    )
    assert "Generating file for 2025-05-02." in capture_cli_logs.text
    assert "Generating file for 2025-05-03." in capture_cli_logs.text


def test_decode_hk_packet_with_data_from_multiple_apids(capture_cli_logs):
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

    processor = instantiate_hk_processor()
    processor.initialize(Path("xtce/tlm_20241024.xml"))

    # Exercise.
    processed_files: dict[Path, IFilePathHandler] = processor.process(packet_path)

    # Verify.
    assert len(processed_files) == 2

    processed_path1, processed_path2 = processed_files.keys()

    assert processed_path1.exists()
    assert processed_path2.exists()

    assert processed_path1.name == "imap_mag_l1_hsk-pw_20250502_v001.csv"
    assert processed_path2.name == "imap_mag_l1_hsk-status_20250502_v001.csv"

    assert f"Found 2 ApIDs (1063, 1064) in {packet_path}." in capture_cli_logs.text
    assert (
        "Splitting data for ApID 1063 (MAG_HSK_PW) into separate files for each day:"
        in capture_cli_logs.text
    )
    assert (
        "Splitting data for ApID 1064 (MAG_HSK_STATUS) into separate files for each day:"
        in capture_cli_logs.text
    )


def test_decode_hk_packet_data_already_exists_in_datastore(capture_cli_logs):
    """Test that HKProcessor loads existing data and includes it in the output file (without duplicates)."""

    # Set up.
    packet_path = Path("tests/data/2025/MAG_HSK_PW_20251017_sclk.pkts")

    processor = instantiate_hk_processor()
    processor.initialize(Path("xtce/tlm_20241024.xml"))

    # Exercise.
    processed_files: dict[Path, IFilePathHandler] = processor.process(packet_path)

    # Verify.
    assert len(processed_files) == 1

    processed_path: Path = next(iter(processed_files))

    assert processed_path.exists()
    assert processed_path.name == "imap_mag_l1_hsk-pw_20251017_v001.csv"

    assert (
        "Found 1 existing files for MAG_HSK_PW on 2025-10-17." in capture_cli_logs.text
    )
    assert (
        "Merging new data with existing data for 2025-10-17." in capture_cli_logs.text
    )

    df = pd.read_csv(processed_path, index_col=0)

    assert df.shape[0] == 918
    assert all(df.index == df.index.unique())


def test_decode_hk_packet_groupby_returns_tuple_for_day():
    """Very specific test to check that we support the `groupby` method returning a tuple for the `day` parameter."""

    # Set up.
    packet_path = Path("tests/data/2025/groupby_day_as_tuple.bin")

    processor = instantiate_hk_processor()
    processor.initialize(Path("xtce/tlm_20241024.xml"))

    # Exercise.
    processed_files: dict[Path, IFilePathHandler] = processor.process(packet_path)

    # Verify.
    assert len(processed_files) == 1

    processed_path = next(iter(processed_files))
    assert processed_path.exists()

    assert processed_path.name == "imap_mag_l1_hsk-status_20250331_v001.csv"
