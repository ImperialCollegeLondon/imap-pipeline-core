import os
import re
from datetime import datetime, timedelta

import pytest

from imap_db.model import WorkflowProgress
from imap_mag.config.AppSettings import AppSettings
from imap_mag.download.FetchBinary import FetchBinary
from imap_mag.process.HKProcessor import HKProcessor
from imap_mag.util import Environment, HKPacket
from prefect_server.pollHK import poll_hk_flow
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    BEGINNING_OF_IMAP,
    END_OF_TODAY,
    NOW,
    TEST_DATA,
    TODAY,
    mock_datetime_provider,  # noqa: F401
)
from tests.util.prefect import prefect_test_fixture  # noqa: F401


def define_available_data_webpoda_mappings(
    wiremock_manager,
    packet: str,
    start_date: str,
    end_date: str,
    binary_file: str,
    ert_timestamp: datetime,
    actual_timestamp: datetime,
    use_ert: bool = True,
):
    time_var = "ert" if use_ert else "time"

    wiremock_manager.add_file_mapping(
        f"/packets/SID2/{packet}.bin?{time_var}%3E={start_date}&{time_var}%3C{end_date}&project(packet)",
        binary_file,
        priority=1,
    )
    wiremock_manager.add_string_mapping(
        f"/packets/SID2/{packet}.csv?{time_var}%3E={start_date}&{time_var}%3C{end_date}&project(ert)&formatTime(%22yyyy-MM-dd'T'HH:mm:ss%22)",
        f"ert\n{ert_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}\n",
        priority=1,
    )
    wiremock_manager.add_string_mapping(
        f"/packets/SID2/{packet}.csv?{time_var}%3E={start_date}&{time_var}%3C{end_date}&project(time)&formatTime(%22yyyy-MM-dd'T'HH:mm:ss%22)",
        f"time\n{actual_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}\n",
        priority=1,
    )


def define_unavailable_data_webpoda_mappings(wiremock_manager):
    empty_file = os.path.abspath(str(TEST_DATA / "EMPTY_HK.pkts"))

    wiremock_manager.add_file_mapping(
        re.escape("/packets/SID2/")
        + r".*"
        + re.escape(".bin?")
        + r"(?:ert|time)"
        + re.escape("%3E=")
        + r".*"
        + re.escape("&")
        + r"(?:ert|time)"
        + r".*"
        + re.escape("&project(packet)"),
        empty_file,
        is_pattern=True,
        priority=2,
    )

    wiremock_manager.add_string_mapping(
        re.escape("/packets/SID2/")
        + r".*"
        + re.escape(".csv?")
        + r"(?:ert|time)"
        + re.escape("%3E=")
        + r".*"
        + re.escape("&")
        + r"(?:ert|time)"
        + re.escape("%3C")
        + r".*"
        + re.escape("&project(")
        + r"(?:ert|time)"
        + re.escape(")&formatTime(%22yyyy-MM-dd'T'HH:mm:ss%22)"),
        "time_var\n",
        is_pattern=True,
        priority=2,
    )


def verify_not_requested_hk(database, not_requested_hk: list[HKPacket]):
    progress_items = database.get_all_workflow_progress()
    for hk in not_requested_hk:
        # find the matching progress item
        workflow_progress = next(
            (item for item in progress_items if item.item_name == hk.packet_name),
            WorkflowProgress(item_name=hk.packet_name),
        )

        assert workflow_progress.get_last_checked_date() is None
        assert workflow_progress.get_progress_timestamp() is None


def verify_not_available_hk(database, not_available_hk: list[HKPacket]):
    progress_items = database.get_all_workflow_progress()
    for hk in not_available_hk:
        # find the matching progress item
        workflow_progress = next(
            (item for item in progress_items if item.item_name == hk.packet_name),
            WorkflowProgress(item_name=hk.packet_name),
        )

        assert workflow_progress.get_last_checked_date() == NOW
        assert workflow_progress.get_progress_timestamp() is None


def verify_available_hk(
    database,
    available_hk: list[HKPacket],
    ert_timestamp: datetime,
    actual_timestamp: datetime,
):
    for hk in available_hk:
        # Database.
        workflow_progress = database.get_workflow_progress(hk.packet_name)

        assert workflow_progress.get_last_checked_date() == NOW
        assert workflow_progress.get_progress_timestamp() == ert_timestamp

    # Files.
    check_file_existence(available_hk, actual_timestamp)


def check_file_existence(
    hk_to_check: list[HKPacket],
    actual_timestamp: datetime,
):
    datastore = AppSettings().data_store
    for hk in hk_to_check:
        descriptor = hk.packet_name.lstrip("MAG_").lower().replace("_", "-")

        bin_folder = os.path.join(
            datastore, "hk/mag/l0", f"{descriptor}", actual_timestamp.strftime("%Y/%m")
        )
        bin_file = (
            f"imap_mag_l0_{descriptor}_{actual_timestamp.strftime('%Y%m%d')}_001.pkts"
        )

        csv_folder = os.path.join(
            datastore, "hk/mag/l1", descriptor, actual_timestamp.strftime("%Y/%m")
        )
        csv_file = (
            f"imap_mag_l1_{descriptor}_{actual_timestamp.strftime('%Y%m%d')}_v001.csv"
        )

        assert os.path.exists(os.path.join(bin_folder, bin_file))
        assert os.path.exists(os.path.join(csv_folder, csv_file))


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_hk_autoflow_first_ever_run(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811,
    dynamic_work_folder,
    clean_datastore,
):
    # Set up.
    binary_files: dict[str, str] = {
        "MAG_HSK_PW": os.path.abspath(str(TEST_DATA / "MAG_HSK_PW.pkts")),
        "MAG_HSK_STATUS": os.path.abspath(str(TEST_DATA / "MAG_HSK_STATUS.pkts")),
        "MAG_HSK_PROCSTAT": os.path.abspath(str(TEST_DATA / "MAG_HSK_PROCSTAT.pkts")),
    }

    beginning_of_imap = BEGINNING_OF_IMAP.strftime("%Y-%m-%dT%H:%M:%S")
    end_of_today = END_OF_TODAY.strftime("%Y-%m-%dT%H:%M:%S")

    ert_timestamp = datetime(2025, 4, 2, 13, 37, 9)
    actual_timestamp = datetime(2025, 5, 2, 11, 37, 9)

    available_hk: list[HKPacket] = [
        HKPacket.SID3_PW,
        HKPacket.SID4_STATUS,
        HKPacket.SID11_PROCSTAT,
    ]
    not_available_hk: list[HKPacket] = list(
        {p for p in HKPacket}.difference(available_hk)
    )

    wiremock_manager.reset()

    # Some data is available only for specific packets.
    for hk in available_hk:
        define_available_data_webpoda_mappings(
            wiremock_manager,
            packet=hk.packet_name,
            start_date=beginning_of_imap,
            end_date=end_of_today,
            binary_file=binary_files[hk.packet_name],
            ert_timestamp=ert_timestamp,
            actual_timestamp=actual_timestamp,
        )

    # No data is available for any other date/packet.
    define_unavailable_data_webpoda_mappings(wiremock_manager)

    # Exercise.
    with Environment(
        MAG_FETCH_BINARY_API_URL_BASE=wiremock_manager.get_url(),
        IMAP_WEBPODA_TOKEN="12345",
    ):
        await poll_hk_flow(hk_packets=available_hk + not_available_hk)

    # Verify.
    verify_not_available_hk(test_database, not_available_hk)
    verify_available_hk(
        test_database,
        available_hk,
        ert_timestamp,
        actual_timestamp,
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_hk_autoflow_continue_from_previous_download(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    # Set up.
    binary_files: dict[str, str] = {
        "MAG_HSK_PW": os.path.abspath(str(TEST_DATA / "MAG_HSK_PW.pkts")),
    }

    progress_timestamp = TODAY + timedelta(hours=5, minutes=30)
    end_of_today = END_OF_TODAY.strftime("%Y-%m-%dT%H:%M:%S")

    ert_timestamp = progress_timestamp + timedelta(hours=1, minutes=37, seconds=9)
    actual_timestamp = datetime(2025, 5, 2, 11, 37, 9)

    available_hk: list[HKPacket] = [
        HKPacket.SID3_PW,
    ]

    wiremock_manager.reset()

    # Some data is available only for specific packets.
    for hk in available_hk:
        workflow_progress = test_database.get_workflow_progress(hk.packet_name)
        workflow_progress.update_progress_timestamp(progress_timestamp)
        test_database.save(workflow_progress)

        define_available_data_webpoda_mappings(
            wiremock_manager,
            packet=hk.packet_name,
            start_date=progress_timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
            end_date=end_of_today,
            binary_file=binary_files[hk.packet_name],
            ert_timestamp=ert_timestamp,
            actual_timestamp=actual_timestamp,
        )

    # No data is available for any other date/packet.
    define_unavailable_data_webpoda_mappings(wiremock_manager)

    # Exercise.
    with Environment(
        MAG_FETCH_BINARY_API_URL_BASE=wiremock_manager.get_url(),
        IMAP_WEBPODA_TOKEN="12345",
    ):
        await poll_hk_flow(hk_packets=available_hk)

    # Verify.
    verify_available_hk(
        test_database,
        available_hk,
        ert_timestamp,
        actual_timestamp,
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
@pytest.mark.parametrize("force_database_update", [True, False])
async def test_poll_hk_specify_packets_and_start_end_dates(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811,
    force_database_update,
    capture_cli_logs,
    dynamic_work_folder,
    clean_datastore,
):
    # Set up.
    binary_files: dict[str, str] = {
        "MAG_HSK_STATUS": os.path.abspath(str(TEST_DATA / "MAG_HSK_STATUS.pkts")),
        "MAG_HSK_SCI": os.path.abspath(str(TEST_DATA / "MAG_HSK_SCI.pkts")),
    }

    start_date = datetime(2025, 5, 1)
    end_date = datetime(2025, 5, 2)
    actual_end_date_for_download = datetime(2025, 5, 3)

    ert_timestamp = datetime(2025, 5, 2, 13, 37, 9)
    actual_timestamp = datetime(2025, 5, 2, 11, 37, 9)

    available_hk: list[HKPacket] = [
        HKPacket.SID4_STATUS,
        HKPacket.SID5_SCI,
    ]
    not_available_hk: list[HKPacket] = [HKPacket.SID11_PROCSTAT, HKPacket.SID15]

    requested_hk: list[HKPacket] = available_hk + not_available_hk

    wiremock_manager.reset()

    # Some data is available for the requested dates, only for specific packets.
    for hk in available_hk:
        define_available_data_webpoda_mappings(
            wiremock_manager,
            packet=hk.packet_name,
            start_date=start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            end_date=actual_end_date_for_download.strftime("%Y-%m-%dT%H:%M:%S"),
            binary_file=binary_files[hk.packet_name],
            ert_timestamp=ert_timestamp,
            actual_timestamp=actual_timestamp,
            use_ert=False,
        )

    # No data is available for any other date/packet.
    define_unavailable_data_webpoda_mappings(wiremock_manager)

    # Exercise.
    with Environment(
        MAG_FETCH_BINARY_API_URL_BASE=wiremock_manager.get_url(),
        IMAP_WEBPODA_TOKEN="12345",
    ):
        await poll_hk_flow(
            start_date=start_date,
            end_date=end_date,
            hk_packets=requested_hk,
            force_database_update=force_database_update,
        )

    # Verify.
    check_file_existence(available_hk, actual_timestamp)

    if force_database_update:
        assert (
            "Database cannot be updated without forcing ERT. Database will not be updated."
            in capture_cli_logs.text
        )

    # Database should not be updated when non-ERT start and end dates are provided.
    verify_not_requested_hk(test_database, [p for p in HKPacket])


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_hk_specify_ert_start_end_dates(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811,
    dynamic_work_folder,
    clean_datastore,
):
    # Set up.
    binary_files: dict[str, str] = {
        "MAG_HSK_STATUS": os.path.abspath(str(TEST_DATA / "MAG_HSK_STATUS.pkts")),
        "MAG_HSK_SCI": os.path.abspath(str(TEST_DATA / "MAG_HSK_SCI.pkts")),
    }

    start_date = datetime(2025, 5, 1)
    end_date = datetime(2025, 5, 2)
    actual_end_date_for_download = datetime(2025, 5, 3)

    ert_timestamp = datetime(2025, 5, 2, 13, 37, 9)
    actual_timestamp = datetime(2025, 5, 2, 11, 37, 9)

    available_hk: list[HKPacket] = [
        HKPacket.SID4_STATUS,
        HKPacket.SID5_SCI,
    ]
    not_available_hk: list[HKPacket] = [HKPacket.SID11_PROCSTAT, HKPacket.SID15]

    requested_hk: list[HKPacket] = available_hk + not_available_hk
    not_requested_hk: list[HKPacket] = list(
        {p for p in HKPacket}.difference(requested_hk)
    )

    wiremock_manager.reset()

    # Some data is available for the requested dates, only for specific packets.
    for hk in available_hk:
        define_available_data_webpoda_mappings(
            wiremock_manager,
            packet=hk.packet_name,
            start_date=start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            end_date=actual_end_date_for_download.strftime("%Y-%m-%dT%H:%M:%S"),
            binary_file=binary_files[hk.packet_name],
            ert_timestamp=ert_timestamp,
            actual_timestamp=actual_timestamp,
        )

    # No data is available for any other date/packet.
    define_unavailable_data_webpoda_mappings(wiremock_manager)

    # Exercise.
    with Environment(
        MAG_FETCH_BINARY_API_URL_BASE=wiremock_manager.get_url(),
        IMAP_WEBPODA_TOKEN="12345",
    ):
        await poll_hk_flow(
            start_date=start_date,
            end_date=end_date,
            hk_packets=requested_hk,
            force_database_update=True,
            force_ert=True,
        )

    # Verify.
    verify_not_requested_hk(test_database, not_requested_hk)
    verify_not_available_hk(test_database, not_available_hk)
    verify_available_hk(
        test_database,
        available_hk,
        ert_timestamp,
        actual_timestamp,
    )


@pytest.fixture(scope="function")
def mock_functionality_to_fail_on_call(monkeypatch, function_to_mock):
    def throw_error_on_call(*args, **kwargs):
        raise RuntimeError("FetchBinary download failed for testing purposes.")

    match function_to_mock:
        case "FetchBinary":
            monkeypatch.setattr(FetchBinary, "download_binaries", throw_error_on_call)
        case "HKProcessor":
            monkeypatch.setattr(HKProcessor, "process", throw_error_on_call)


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
@pytest.mark.parametrize("function_to_mock", ["FetchBinary", "HKProcessor"])
async def test_database_progress_table_not_modified_if_poll_hk_fails(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811,
    mock_functionality_to_fail_on_call,
):
    # Set up.
    binary_files: dict[str, str] = {
        "MAG_HSK_PW": os.path.abspath(str(TEST_DATA / "MAG_HSK_PW.pkts")),
        "MAG_HSK_STATUS": os.path.abspath(str(TEST_DATA / "MAG_HSK_STATUS.pkts")),
        "MAG_HSK_PROCSTAT": os.path.abspath(str(TEST_DATA / "MAG_HSK_PROCSTAT.pkts")),
    }

    beginning_of_imap = BEGINNING_OF_IMAP.strftime("%Y-%m-%dT%H:%M:%S")
    end_of_today = END_OF_TODAY.strftime("%Y-%m-%dT%H:%M:%S")

    ert_timestamp = datetime(2025, 4, 2, 13, 37, 9)
    actual_timestamp = datetime(2025, 5, 2, 11, 37, 9)

    hk_to_poll: list[HKPacket] = [
        HKPacket.SID3_PW,
    ]

    wiremock_manager.reset()

    # Some data is available only for specific packets.
    for hk in hk_to_poll:
        define_available_data_webpoda_mappings(
            wiremock_manager,
            packet=hk.packet_name,
            start_date=beginning_of_imap,
            end_date=end_of_today,
            binary_file=binary_files[hk.packet_name],
            ert_timestamp=ert_timestamp,
            actual_timestamp=actual_timestamp,
        )

    # No data is available for any other date/packet.
    define_unavailable_data_webpoda_mappings(wiremock_manager)

    # Exercise.
    with (
        pytest.raises(
            RuntimeError,
            match=re.escape("FetchBinary download failed for testing purposes."),
        ),
        Environment(
            MAG_FETCH_BINARY_API_URL_BASE=wiremock_manager.get_url(),
            IMAP_WEBPODA_TOKEN="12345",
        ),
    ):
        await poll_hk_flow(
            hk_packets=hk_to_poll,
        )

    # Verify.
    progress_items = test_database.get_all_workflow_progress()
    for hk in [p for p in HKPacket]:
        # find the matching progress item
        workflow_progress = next(
            (item for item in progress_items if item.item_name == hk.packet_name),
            WorkflowProgress(item_name=hk.packet_name),
        )

        assert workflow_progress.get_last_checked_date() is None
        assert workflow_progress.get_progress_timestamp() is None
