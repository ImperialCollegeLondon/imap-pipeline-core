"""Shared test helpers for poll-* flows that download from WebTCAD LaTiS."""

import os
import re
from datetime import datetime

from imap_mag.client.WebTCADLaTiS import HKWebTCADItems
from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    FetchByDatesRunParameters,
    ProgressUpdateMode,
)
from imap_mag.util import Environment

WEBTCAD_URL_ENDPOINT_PATH = "/AnalogTelemetryItem_SID1"

SAMPLE_CSV_WITH_DATA = (
    "time,value\n"
    "2026-02-11T14:00:00.000,1.23\n"
    "2026-02-11T14:01:00.000,2.34\n"
    "2026-02-11T14:02:00.000,3.45\n"
)

SAMPLE_CSV_EMPTY = "time,value\n"


def define_available_latis_mapping(
    wiremock_manager,
    item: HKWebTCADItems,
    start_date: datetime,
    end_date: datetime,
    csv_content: str,
) -> None:
    """Add a WireMock mapping that returns ``csv_content`` for the given item and date range."""
    start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    url = (
        f"{WEBTCAD_URL_ENDPOINT_PATH}.csv"
        f"?TMID={item.tmid}"
        f"&time,value"
        f"&time%3E={start_str}"
        f"&time%3C={end_str}"
        f"&format_time(yyyy-MM-dd'T'HH:mm:ss.SSS)"
    )

    wiremock_manager.add_string_mapping(url, csv_content, priority=1)


def define_unavailable_latis_mapping(wiremock_manager, item: HKWebTCADItems) -> None:
    """Add a fallback WireMock mapping that returns an empty CSV for the given item."""
    wiremock_manager.add_string_mapping(
        re.escape(f"{WEBTCAD_URL_ENDPOINT_PATH}.csv?TMID={item.tmid}") + r".*",
        SAMPLE_CSV_EMPTY,
        is_pattern=True,
        priority=2,
    )


def datastore_csv_path(item: HKWebTCADItems, date: datetime) -> str:
    """Return the absolute datastore path of the CSV the pipeline should produce for ``date``."""
    instrument = item.instrument.short_name
    folder = os.path.join(
        AppSettings().data_store,
        f"hk/{instrument}/l1",
        item.descriptor,
        date.strftime("%Y/%m"),
    )
    filename = (
        f"imap_{instrument}_l1_{item.descriptor}_{date.strftime('%Y%m%d')}_v001.csv"
    )
    return os.path.join(folder, filename)


def assert_file_exists(item: HKWebTCADItems, date: datetime) -> None:
    """Assert that the pipeline produced a CSV in the datastore for ``date``."""
    path = datastore_csv_path(item, date)
    assert os.path.exists(path), f"Expected file not found at {path}"


def assert_file_not_exists(item: HKWebTCADItems, date: datetime) -> None:
    """Assert the pipeline did NOT produce a CSV in the datastore for ``date``."""
    path = datastore_csv_path(item, date)
    assert not os.path.exists(path), f"Unexpected file exists at {path}"


async def execute_webtcad_flow(
    flow,
    *,
    wiremock_manager,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    force_redownload: bool = False,
    use_database: bool = True,
    prefect_test_fixture=None,
    do_not_update_progress: bool = False,
) -> None:
    """Run a WebTCAD poll flow against the supplied WireMock instance.

    If ``prefect_test_fixture`` is supplied the decorated flow is invoked (so tests
    exercising the Prefect runtime work); otherwise the underlying ``.fn`` is called
    directly to keep the test fast.
    """
    with Environment(
        MAG_FETCH_WEBTCAD_API_URL_BASE=wiremock_manager.get_url(),
        IMAP_WEBPODA_TOKEN="12345",
    ):
        func_under_test = flow if prefect_test_fixture else flow.fn

        mode = (
            ProgressUpdateMode.NEVER_UPDATE_PROGRESS
            if do_not_update_progress
            else ProgressUpdateMode.AUTO_UPDATE_PROGRESS_IF_NEWER
        )

        if start_date is None and end_date is None and not force_redownload:
            await func_under_test(
                run_parameters=AutomaticRunParameters(progress_mode=mode),
                use_database=use_database,
            )
        else:
            await func_under_test(
                run_parameters=FetchByDatesRunParameters(
                    start_date=start_date,
                    end_date=end_date,
                    force_redownload=force_redownload,
                    progress_mode=mode,
                ),
                use_database=use_database,
            )
