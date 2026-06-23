import json
import os
import re
from datetime import datetime

import pytest

from imap_mag.config.AppSettings import AppSettings
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    FetchByDatesRunParameters,
)
from imap_mag.data_pipelines.SmallForcesPipeline import SmallForcesPipeline
from imap_mag.io.file.SmallForcesPathHandler import SmallForcesPathHandler
from imap_mag.util.DatetimeProvider import DatetimeProvider
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    NOW,
)
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401

PROGRESS_ITEM_ID = SmallForcesPipeline.PROGRESS_ITEM_ID

SAMPLE_SMALL_FORCES_FILE_CONTENT = """MISSION_NAME,IMAP
EVENT_NAME,
DSN_SPACECRAFT_ID,-43
PRODUCTION_TIME,2026-02-05 00:16:40 (UTC)
PRODUCER_ID,JHU/APL
FILE_TYPE,SFF
START_TIME,2026 FEB 03 23:57:05.402152 (UTC)
QUAT_CONVENTION,SPICE ([QS Q1 Q2 Q3])
QUAT_FORMAT,EMO2000_TO_BODY
DV_FRAME,EMO2000
DV_UNITS,m/s
DTIME_UNITS,s
DMASS_UNITS,kg
THR_DUR_UNITS,msec
PROPMASS_UNITS,kg
lPROPRESS_UNITS,Pa
TOTAL_SC_MASS_UNITS,kg
STARTTIME/ENDTIME_TIMEZONE,UTC

INDEX,RecType,INBURN,STARTTIME,ENDTIME,DTIME,DMASS,DVX,DVY,DVZ,QUAT_S,QUAT_1,QUAT_2,QUAT_3,CONTROL_MODE,THR_A1_DUR,THR_A3_DUR,THR_R1_DUR,THR_R3_DUR,THR_R5_DUR,THR_R7_DUR,THR_A2_DUR,THR_A4_DUR,THR_R2_DUR,THR_R4_DUR,THR_R6_DUR,THR_R8_DUR,PROP_MASS,PROP_PRESSURE,TOTAL_SC_MASS
$$EOH
1, H, 0, 2026 FEB 03 23:57:06.402151, 2026 FEB 03 23:57:07.402151,    1.000,  0.000000,  0.000000000,  0.000000000,  0.000000000, -0.15354452973,  0.56773148362, -0.42271123356,  0.68950725371, 0,       0.0,       0.0,       0.0,       0.0,       0.0,       0.0,       0.0,       0.0,       0.0,       0.0,       0.0,       0.0, 141.606506, 1942692.120000, 794.466506"""

SAMPLE_SMALL_FORCES_API_RESPONSE = [
    {
        "file_path": "imap/spice/activities/imap_2026_060_2026_061_hist_01.sff",
        "start_date": "2026-03-01, 00:00:00",
        "end_date": "2026-03-02, 00:00:00",
        "version": "01",
        "ingestion_date": "2026-03-02, 09:35:25",
    },
    {
        "file_path": "imap/spice/activities/imap_2026_061_2026_061_hist_01.sff",
        "start_date": "2026-03-02, 00:00:00",
        "end_date": "2026-03-02, 00:00:00",
        "version": "01",
        "ingestion_date": "2026-03-02, 09:35:27",
    },
]

SMALL_FORCES_API_PATH = "/small-forces-table"


def define_small_forces_api_mapping(
    wiremock_manager,
    response_data: list[dict],
):
    """Add WireMock mapping for the small forces table API query."""
    wiremock_manager.add_string_mapping(
        re.escape(f"{SMALL_FORCES_API_PATH}?") + r".*",
        json.dumps(response_data),
        is_pattern=True,
        priority=1,
    )


def define_small_forces_download_mapping(
    wiremock_manager,
    file_path: str,
    content: str = SAMPLE_SMALL_FORCES_FILE_CONTENT,
):
    """Add WireMock mapping for downloading a small forces file."""
    # The download URL strips the 'imap/' prefix since imap_data_access handles it
    wiremock_manager.add_string_mapping(
        f"/download/{file_path}",
        content,
        priority=1,
    )


def define_empty_small_forces_api_mapping(wiremock_manager):
    """Add WireMock mapping that returns empty response for any small forces table query."""
    wiremock_manager.add_string_mapping(
        re.escape(f"{SMALL_FORCES_API_PATH}?") + r".*",
        json.dumps([]),
        is_pattern=True,
        priority=2,
    )


def check_small_forces_file_existence(filename: str, negate=False):
    """Verify that a small forces file exists in the datastore."""
    datastore_path = AppSettings().data_store
    small_forces_file_path = datastore_path / "spice" / "activities" / filename

    if negate:
        assert not small_forces_file_path.exists(), (
            f"File {filename} should not exist in {small_forces_file_path.parent}"
        )
    else:
        assert small_forces_file_path.exists(), (
            f"Expected file {filename} not found in {small_forces_file_path.parent}"
        )


def test_small_forces_table_path_handler_from_filename():
    """Test SmallForcesPathHandler can parse small forces table filenames."""
    handler = SmallForcesPathHandler.from_filename("imap_2026_060_2026_061_hist_01.sff")
    assert handler is not None
    assert handler.filename == "imap_2026_060_2026_061_hist_01.sff"
    assert handler.version == 1
    assert handler.content_date == datetime(2026, 3, 1)
    assert handler.get_folder_structure() == "spice/activities"


def test_small_forces_table_path_handler_with_metadata():
    """Test SmallForcesPathHandler stores API metadata correctly."""
    handler = SmallForcesPathHandler.from_filename("imap_2026_060_2026_061_hist_01.sff")
    assert handler is not None

    metadata = {
        "file_path": "imap/spice/activities/imap_2026_060_2026_061_hist_01.sff",
        "start_date": "2026-03-01, 00:00:00",
        "end_date": "2026-03-02, 00:00:00",
        "version": "01",
        "ingestion_date": "2026-04-01, 01:50:09",
    }
    handler.add_metadata(metadata)

    assert handler.get_metadata() == metadata
    assert handler.version == 1
    assert handler.content_date == datetime(2026, 3, 1)


def test_small_forces_path_handler_extracts_version_from_filename():
    """Test SmallForcesPathHandler extracts version correctly from filename."""
    handler_v1 = SmallForcesPathHandler.from_filename(
        "imap_2026_060_2026_061_hist_01.sff"
    )
    assert handler_v1 is not None
    assert handler_v1.version == 1

    handler_v99 = SmallForcesPathHandler.from_filename(
        "imap_2026_060_2026_061_hist_99.sff"
    )
    assert handler_v99 is not None
    assert handler_v99.version == 99

    assert handler_v1.supports_sequencing() is False


def test_small_forces_path_handler_returns_none_for_non_spin_files():
    """Test SmallForcesPathHandler returns None for non-small-forces files."""
    assert (
        SmallForcesPathHandler.from_filename("imap_2026_089_2026_090_01.ah.bc") is None
    )
    assert SmallForcesPathHandler.from_filename("random_file.csv") is None
    assert (
        SmallForcesPathHandler.from_filename("imap_2026_060_2026_061_hist_99.csv")
        is None
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_small_forces_first_ever_run(
    wiremock_manager,
    test_database,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """On first run with no progress, download small forces tables from beginning of IMAP to today."""
    datetime_provider = DatetimeProvider(fixed_now=NOW)
    wiremock_manager.reset()

    # Create response data with dates matching BEGINNING_OF_IMAP
    api_response = [
        {
            "file_path": "imap/spice/activities/imap_2025_283_2025_284_hist_01.sff",
            "start_date": "2025-10-10, 00:00:00",
            "end_date": "2025-10-11, 00:00:00",
            "version": "01",
            "ingestion_date": "2025-10-13, 18:05:10",
        }
    ]

    define_small_forces_api_mapping(wiremock_manager, api_response)
    define_small_forces_download_mapping(
        wiremock_manager,
        "imap/spice/activities/imap_2025_283_2025_284_hist_01.sff",
    )

    await execute_pipeline_under_test(
        wiremock_manager, test_database, datetime_provider=datetime_provider
    )

    check_small_forces_file_existence("imap_2025_283_2025_284_hist_01.sff")

    # Verify workflow progress was updated
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() == datetime(
        2025, 10, 13, 18, 5, 10
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_small_forces_no_new_data(
    wiremock_manager,
    test_database,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """When API returns no files, progress timestamp stays unchanged."""
    datetime_provider = DatetimeProvider(fixed_now=NOW)
    wiremock_manager.reset()

    define_empty_small_forces_api_mapping(wiremock_manager)

    await execute_pipeline_under_test(
        wiremock_manager, test_database, datetime_provider=datetime_provider
    )

    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() is None


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_small_forces_manual_date_range(
    wiremock_manager,
    test_database,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """Manually specify date range to download specific small forces table files."""
    datetime_provider = DatetimeProvider(fixed_now=NOW)
    wiremock_manager.reset()

    start_date = datetime(2026, 4, 1)
    end_date = datetime(2026, 4, 5)

    api_response = [
        {
            "file_path": "imap/spice/activities/imap_2026_091_2026_092_hist_01.sff",
            "start_date": "2026-04-01, 00:00:00",
            "end_date": "2026-04-02, 00:00:00",
            "version": "01",
            "ingestion_date": "2026-04-03, 21:35:17",
        },
    ]

    define_small_forces_api_mapping(wiremock_manager, api_response)
    define_small_forces_download_mapping(
        wiremock_manager,
        "imap/spice/activities/imap_2026_091_2026_092_hist_01.sff",
    )

    await execute_pipeline_under_test(
        wiremock_manager,
        test_database,
        start_date=start_date,
        end_date=end_date,
        datetime_provider=datetime_provider,
    )

    check_small_forces_file_existence("imap_2026_091_2026_092_hist_01.sff")

    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() == datetime(
        2026, 4, 3, 21, 35, 17
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_small_forces_continue_from_previous(
    wiremock_manager,
    test_database,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """On subsequent run, start from after last progress."""
    datetime_provider = DatetimeProvider(fixed_now=NOW)
    wiremock_manager.reset()

    # Set previous progress
    progress_timestamp = datetime(2025, 6, 2, 1, 50, 9)
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    workflow_progress.update_progress_timestamp(progress_timestamp)
    test_database.save(workflow_progress)

    api_response = [
        {
            "file_path": "imap/spice/activities/imap_2025_153_2025_154_hist_01.sff",
            "start_date": "2025-06-02, 00:00:00",
            "end_date": "2025-06-03, 00:00:00",
            "version": "01",
            "ingestion_date": "2025-06-03, 01:50:11",
        },
    ]

    define_small_forces_api_mapping(wiremock_manager, api_response)
    define_small_forces_download_mapping(
        wiremock_manager,
        "imap/spice/activities/imap_2025_153_2025_154_hist_01.sff",
    )

    await execute_pipeline_under_test(
        wiremock_manager, test_database, datetime_provider=datetime_provider
    )

    check_small_forces_file_existence("imap_2025_153_2025_154_hist_01.sff")

    updated_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert updated_progress.get_last_checked_date() == NOW
    assert updated_progress.get_progress_timestamp() == datetime(2025, 6, 3, 1, 50, 11)


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_small_forces_indexes_metadata_in_database(
    wiremock_manager,
    test_database,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """Verify that small forces table metadata is indexed in the database."""
    datetime_provider = DatetimeProvider(fixed_now=NOW)
    wiremock_manager.reset()

    api_response = [
        {
            "file_path": "imap/spice/activities/imap_2025_152_2025_153_hist_01.sff",
            "start_date": "2025-09-24, 00:00:00",
            "end_date": "2025-09-25, 00:00:00",
            "version": "01",
            "ingestion_date": "2025-09-25, 01:50:09",
        },
    ]

    define_small_forces_api_mapping(wiremock_manager, api_response)
    define_small_forces_download_mapping(
        wiremock_manager,
        "imap/spice/activities/imap_2025_152_2025_153_hist_01.sff",
    )

    await execute_pipeline_under_test(
        wiremock_manager, test_database, datetime_provider=datetime_provider
    )

    # Verify the file is in the database with metadata
    files = test_database.get_files_by_path("spice/activities")
    assert len(files) == 1
    assert files[0].file_meta is not None
    assert files[0].file_meta["start_date"] == "2025-09-24, 00:00:00"
    assert files[0].file_meta["end_date"] == "2025-09-25, 00:00:00"
    assert files[0].file_meta["version"] == "01"
    assert files[0].file_meta["ingestion_date"] == "2025-09-25, 01:50:09"
    assert files[0].version == 1


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_small_forces_without_database(
    wiremock_manager,
    test_database,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """Test that the pipeline works without a database."""
    datetime_provider = DatetimeProvider(fixed_now=NOW)
    wiremock_manager.reset()

    api_response = [
        {
            "file_path": "imap/spice/activities/imap_2025_152_2025_153_hist_01.sff",
            "start_date": "2025-09-24, 00:00:00",
            "end_date": "2025-09-25, 00:00:00",
            "version": "01",
            "ingestion_date": "2025-09-25, 01:50:09",
        },
    ]

    define_small_forces_api_mapping(wiremock_manager, api_response)
    define_small_forces_download_mapping(
        wiremock_manager,
        "imap/spice/activities/imap_2025_152_2025_153_hist_01.sff",
    )

    await execute_pipeline_under_test(
        wiremock_manager,
        test_database,
        use_database=False,
        datetime_provider=datetime_provider,
    )

    check_small_forces_file_existence("imap_2025_152_2025_153_hist_01.sff")


async def execute_pipeline_under_test(
    wiremock_manager,
    test_database,  # noqa: F811
    start_date=None,
    end_date=None,
    use_database=True,
    datetime_provider: DatetimeProvider = DatetimeProvider(),
):
    from imap_mag.client.SDCDataAccess import SDCDataAccess
    from imap_mag.util import Environment

    with Environment(
        IMAP_DATA_ACCESS_URL=wiremock_manager.get_url() + "api-key",
        IMAP_API_KEY="test-key",
    ):
        settings = AppSettings()
        database = test_database if use_database else None

        client = SDCDataAccess(
            auth_code=settings.fetch_spice.api.auth_code,
            data_dir=settings.setup_work_folder_for_command(settings.fetch_spice),
            sdc_url=settings.fetch_spice.api.url_base,
        )

        if start_date or end_date:
            run_params = FetchByDatesRunParameters(
                start_date=start_date,
                end_date=end_date,
            )
        else:
            run_params = AutomaticRunParameters()

        pipeline = SmallForcesPipeline(
            database=database,
            settings=settings,
            client=client,
            datetime_provider=datetime_provider,
        )
        pipeline.build(run_params)
        await pipeline.run()
