import os
import re
from datetime import datetime, timedelta

import pytest

from imap_mag.util import HKPacket
from prefect_server.pollHK import poll_hk_flow
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    END_OF_TODAY,
    NOW,
    TODAY,
    enableLogging,  # noqa: F401
    mock_datetime_provider,  # noqa: F401
    set_env,
    tidyDataFolders,  # noqa: F401
)
from tests.util.prefect import prefect_test_fixture  # noqa: F401


def define_unavailable_data_webpoda_mappings(wiremock_manager):
    empty_file = os.path.abspath("tests/data/2025/EMPTY_HK.pkts")

    wiremock_manager.add_file_mapping(
        re.escape("/packets/SID2/")
        + ".*"
        + re.escape(".bin?ert%3E=")
        + ".*"
        + re.escape("&ert%3C")
        + ".*"
        + re.escape("&project(packet)"),
        empty_file,
        is_pattern=True,
        priority=2,
    )

    wiremock_manager.add_string_mapping(
        re.escape("/packets/SID2/")
        + ".*"
        + re.escape(".csv?ert%3E=")
        + ".*"
        + re.escape("&ert%3C")
        + ".*"
        + re.escape("&project(")
        + "(?:ert|time)"
        + re.escape(")&formatTime(%22yyyy-MM-dd'T'HH:mm:ss%22)"),
        "time_var\n",
        is_pattern=True,
        priority=2,
    )


def verify_not_available_hk(database, not_available_hk: list[HKPacket]):
    for hk_type in not_available_hk:
        download_progress = database.get_download_progress(hk_type.packet)

        assert download_progress.get_last_checked_date() == NOW
        assert download_progress.get_progress_timestamp() is None


def verify_available_hk(
    database,
    available_hk: list[HKPacket],
    ert_timestamp: datetime,
    actual_timestamp: datetime,
):
    for hk_type in available_hk:
        # Database.
        download_progress = database.get_download_progress(hk_type.packet)

        assert download_progress.get_last_checked_date() == NOW
        assert download_progress.get_progress_timestamp() == ert_timestamp

        # Files.
        data_folder = os.path.join("output", actual_timestamp.strftime("%Y/%m/%d"))
        bin_file = (
            f"imap_mag_{hk_type.packet.lstrip('MAG_').lower().replace('_', '-')}_"
            + f"{actual_timestamp.strftime('%Y%m%d')}_v000.pkts"
        )
        csv_file = (
            f"imap_mag_{hk_type.packet.lstrip('MAG_').lower().replace('_', '-')}_"
            + f"{actual_timestamp.strftime('%Y%m%d')}_v000.csv"
        )

        assert os.path.exists(os.path.join(data_folder, bin_file))
        assert os.path.exists(os.path.join(data_folder, csv_file))


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_hk_autoflow_first_ever_run(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
):
    # Set up.
    binary_file = os.path.abspath("tests/data/2025/MAG_HSK_PW.pkts")

    today = TODAY.strftime("%Y-%m-%dT%H:%M:%S")
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

    # Some data is available for "today", only for specific packets.
    for hk_type in available_hk:
        wiremock_manager.add_file_mapping(
            f"/packets/SID2/{hk_type.packet}.bin?ert%3E={today}&ert%3C{end_of_today}&project(packet)",
            binary_file,
            priority=1,
        )
        wiremock_manager.add_string_mapping(
            f"/packets/SID2/{hk_type.packet}.csv?ert%3E={today}&ert%3C{end_of_today}&project(ert)&formatTime(%22yyyy-MM-dd'T'HH:mm:ss%22)",
            f"ert\n{ert_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}\n",
            priority=1,
        )
        wiremock_manager.add_string_mapping(
            f"/packets/SID2/{hk_type.packet}.csv?ert%3E={today}&ert%3C{end_of_today}&project(time)&formatTime(%22yyyy-MM-dd'T'HH:mm:ss%22)",
            f"time\n{actual_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}\n",
            priority=1,
        )

    # No data is available for any other date/packet.
    define_unavailable_data_webpoda_mappings(wiremock_manager)

    # Exercise.
    with set_env("MAG_FETCH_BINARY_WEBPODA_URL_BASE", wiremock_manager.get_url()):
        await poll_hk_flow()

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
):
    # Set up.
    binary_file = os.path.abspath("tests/data/2025/MAG_HSK_PW.pkts")

    progress_timestamp = TODAY + timedelta(hours=5, minutes=30)
    end_of_today = END_OF_TODAY.strftime("%Y-%m-%dT%H:%M:%S")

    ert_timestamp = progress_timestamp + timedelta(hours=1, minutes=37, seconds=9)
    actual_timestamp = datetime(2025, 5, 2, 11, 37, 9)

    available_hk: list[HKPacket] = [
        HKPacket.SID3_PW,
        HKPacket.SID4_STATUS,
        HKPacket.SID15,
    ]
    not_available_hk: list[HKPacket] = list(
        {p for p in HKPacket}.difference(available_hk)
    )

    wiremock_manager.reset()

    # Some data is available for "today", only for specific packets.
    for hk_type in available_hk:
        download_progress = test_database.get_download_progress(hk_type.packet)
        download_progress.record_successful_download(progress_timestamp)
        test_database.save(download_progress)

        wiremock_manager.add_file_mapping(
            f"/packets/SID2/{hk_type.packet}.bin?ert%3E={progress_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}&ert%3C{end_of_today}&project(packet)",
            binary_file,
            priority=1,
        )
        wiremock_manager.add_string_mapping(
            f"/packets/SID2/{hk_type.packet}.csv?ert%3E={progress_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}&ert%3C{end_of_today}&project(ert)&formatTime(%22yyyy-MM-dd'T'HH:mm:ss%22)",
            f"ert\n{ert_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}\n",
            priority=1,
        )
        wiremock_manager.add_string_mapping(
            f"/packets/SID2/{hk_type.packet}.csv?ert%3E={progress_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}&ert%3C{end_of_today}&project(time)&formatTime(%22yyyy-MM-dd'T'HH:mm:ss%22)",
            f"time\n{actual_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}\n",
            priority=1,
        )

    # No data is available for any other date/packet.
    define_unavailable_data_webpoda_mappings(wiremock_manager)

    # Exercise.
    with set_env("MAG_FETCH_BINARY_WEBPODA_URL_BASE", wiremock_manager.get_url()):
        await poll_hk_flow()

    # Verify.
    verify_not_available_hk(test_database, not_available_hk)
    verify_available_hk(
        test_database,
        available_hk,
        ert_timestamp,
        actual_timestamp,
    )
