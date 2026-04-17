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
from imap_mag.data_pipelines.SpinTablePipeline import SpinTablePipeline
from imap_mag.io.file.SpinTablePathHandler import SpinTablePathHandler
from tests.util.database import test_database  # noqa: F401
from tests.util.miscellaneous import (
    NOW,
    mock_datetime_provider,  # noqa: F401
)
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401

PROGRESS_ITEM_ID = SpinTablePipeline.PROGRESS_ITEM_ID

SAMPLE_SPIN_FILE_CONTENT = "epoch,spin_period,spin_phase,spin_axis_ra,spin_axis_dec\n2026-03-30T00:00:00,15.0,0.0,180.0,90.0\n"

SAMPLE_SPIN_TABLE_API_RESPONSE = [
    {
        "file_path": "imap/spice/spin/imap_2026_152_2026_153_01.spin",
        "start_date": "2026-06-01, 00:00:00",
        "end_date": "2026-06-02, 00:00:00",
        "version": "01",
        "ingestion_date": "2026-06-02, 01:50:09",
    },
    {
        "file_path": "imap/spice/spin/imap_2026_153_2026_154_01.spin",
        "start_date": "2026-06-02, 00:00:00",
        "end_date": "2026-06-03, 00:00:00",
        "version": "01",
        "ingestion_date": "2026-06-03, 01:50:11",
    },
]

SPIN_TABLE_API_PATH = "/spin-table"


def define_spin_table_api_mapping(
    wiremock_manager,
    response_data: list[dict],
):
    """Add WireMock mapping for the spin table API query."""
    wiremock_manager.add_string_mapping(
        re.escape(f"{SPIN_TABLE_API_PATH}?") + r".*",
        json.dumps(response_data),
        is_pattern=True,
        priority=1,
    )


def define_spin_file_download_mapping(
    wiremock_manager,
    file_path: str,
    content: str = SAMPLE_SPIN_FILE_CONTENT,
):
    """Add WireMock mapping for downloading a spin table file."""
    # The download URL strips the 'imap/' prefix since imap_data_access handles it
    wiremock_manager.add_string_mapping(
        f"/download/{file_path}",
        content,
        priority=1,
    )


def define_empty_spin_table_api_mapping(wiremock_manager):
    """Add WireMock mapping that returns empty response for any spin table query."""
    wiremock_manager.add_string_mapping(
        re.escape(f"{SPIN_TABLE_API_PATH}?") + r".*",
        json.dumps([]),
        is_pattern=True,
        priority=2,
    )


def check_spin_file_existence(filename: str, negate=False):
    """Verify that a spin table file exists in the datastore."""
    datastore_path = AppSettings().data_store
    spin_file_path = datastore_path / "spice" / "spin" / filename

    if negate:
        assert not spin_file_path.exists(), (
            f"File {filename} should not exist in {spin_file_path.parent}"
        )
    else:
        assert spin_file_path.exists(), (
            f"Expected file {filename} not found in {spin_file_path.parent}"
        )


def test_spin_table_path_handler_from_filename():
    """Test SpinTablePathHandler can parse spin table filenames."""
    handler = SpinTablePathHandler.from_filename("imap_2026_089_2026_090_01.spin")
    assert handler is not None
    assert handler.filename == "imap_2026_089_2026_090_01.spin"
    assert handler.version == 1
    assert handler.content_date == datetime(2026, 3, 30)
    assert handler.get_folder_structure() == "spice/spin"


def test_spin_table_path_handler_with_metadata():
    """Test SpinTablePathHandler stores API metadata correctly."""
    handler = SpinTablePathHandler.from_filename("imap_2026_089_2026_090_01.spin")
    assert handler is not None

    metadata = {
        "file_path": "imap/spice/spin/imap_2026_089_2026_090_01.spin",
        "start_date": "2026-03-30, 00:00:00",
        "end_date": "2026-03-31, 00:00:00",
        "version": "01",
        "ingestion_date": "2026-04-01, 01:50:09",
    }
    handler.add_metadata(metadata)

    assert handler.get_metadata() == metadata
    assert handler.version == 1
    assert handler.content_date == datetime(2026, 3, 30)


def test_spin_table_path_handler_extracts_version_from_filename():
    """Test SpinTablePathHandler extracts version correctly from filename."""
    handler_v1 = SpinTablePathHandler.from_filename("imap_2025_267_2025_267_01.spin")
    assert handler_v1 is not None
    assert handler_v1.version == 1

    handler_v99 = SpinTablePathHandler.from_filename("imap_2025_267_2025_267_99.spin")
    assert handler_v99 is not None
    assert handler_v99.version == 99

    assert handler_v1.supports_sequencing() is True
    assert handler_v1.get_sequence() == 1


def test_spin_table_path_handler_sequencing():
    """Test SpinTablePathHandler version sequencing updates filename."""
    handler = SpinTablePathHandler.from_filename("imap_2026_089_2026_090_01.spin")
    assert handler is not None
    assert handler.version == 1
    assert handler.get_filename() == "imap_2026_089_2026_090_01.spin"

    handler.increase_sequence()
    assert handler.version == 2
    assert handler.get_filename() == "imap_2026_089_2026_090_02.spin"

    handler.set_sequence(15)
    assert handler.version == 15
    assert handler.get_filename() == "imap_2026_089_2026_090_15.spin"


def test_spin_table_path_handler_returns_none_for_non_spin_files():
    """Test SpinTablePathHandler returns None for non-spin-table files."""
    assert SpinTablePathHandler.from_filename("imap_2026_089_2026_090_01.ah.bc") is None
    assert SpinTablePathHandler.from_filename("random_file.csv") is None
    assert (
        SpinTablePathHandler.from_filename("imap_2026_089_2026_090_01.spin.csv") is None
    )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_spin_table_first_ever_run(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """On first run with no progress, download spin tables from beginning of IMAP to today."""
    wiremock_manager.reset()

    # Create response data with dates matching BEGINNING_OF_IMAP
    api_response = [
        {
            "file_path": "imap/spice/spin/imap_2025_152_2025_153_01.spin",
            "start_date": "2025-06-01, 00:00:00",
            "end_date": "2025-06-02, 00:00:00",
            "version": "01",
            "ingestion_date": "2025-06-02, 01:50:09",
        },
    ]

    define_spin_table_api_mapping(wiremock_manager, api_response)
    define_spin_file_download_mapping(
        wiremock_manager,
        "imap/spice/spin/imap_2025_152_2025_153_01.spin",
    )

    await execute_pipeline_under_test(wiremock_manager, test_database)

    check_spin_file_existence("imap_2025_152_2025_153_01.spin")

    # Verify workflow progress was updated
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() == datetime(2025, 6, 2, 1, 50, 9)


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_spin_table_no_new_data(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """When API returns no files, progress timestamp stays unchanged."""
    wiremock_manager.reset()

    define_empty_spin_table_api_mapping(wiremock_manager)

    await execute_pipeline_under_test(wiremock_manager, test_database)

    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert workflow_progress.get_last_checked_date() == NOW
    assert workflow_progress.get_progress_timestamp() is None


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_spin_table_manual_date_range(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """Manually specify date range to download specific spin table files."""
    wiremock_manager.reset()

    start_date = datetime(2026, 4, 1)
    end_date = datetime(2026, 4, 5)

    api_response = [
        {
            "file_path": "imap/spice/spin/imap_2026_091_2026_092_01.spin",
            "start_date": "2026-04-01, 00:00:00",
            "end_date": "2026-04-02, 00:00:00",
            "version": "01",
            "ingestion_date": "2026-04-03, 21:35:17",
        },
    ]

    define_spin_table_api_mapping(wiremock_manager, api_response)
    define_spin_file_download_mapping(
        wiremock_manager,
        "imap/spice/spin/imap_2026_091_2026_092_01.spin",
    )

    await execute_pipeline_under_test(
        wiremock_manager, test_database, start_date=start_date, end_date=end_date
    )

    check_spin_file_existence("imap_2026_091_2026_092_01.spin")

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
async def test_poll_spin_table_continue_from_previous(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """On subsequent run, start from after last progress."""
    wiremock_manager.reset()

    # Set previous progress
    progress_timestamp = datetime(2025, 6, 2, 1, 50, 9)
    workflow_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    workflow_progress.update_progress_timestamp(progress_timestamp)
    test_database.save(workflow_progress)

    api_response = [
        {
            "file_path": "imap/spice/spin/imap_2025_153_2025_154_01.spin",
            "start_date": "2025-06-02, 00:00:00",
            "end_date": "2025-06-03, 00:00:00",
            "version": "01",
            "ingestion_date": "2025-06-03, 01:50:11",
        },
    ]

    define_spin_table_api_mapping(wiremock_manager, api_response)
    define_spin_file_download_mapping(
        wiremock_manager,
        "imap/spice/spin/imap_2025_153_2025_154_01.spin",
    )

    await execute_pipeline_under_test(wiremock_manager, test_database)

    check_spin_file_existence("imap_2025_153_2025_154_01.spin")

    updated_progress = test_database.get_workflow_progress(PROGRESS_ITEM_ID)
    assert updated_progress.get_last_checked_date() == NOW
    assert updated_progress.get_progress_timestamp() == datetime(2025, 6, 3, 1, 50, 11)


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_spin_table_indexes_metadata_in_database(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """Verify that spin table metadata is indexed in the database."""
    wiremock_manager.reset()

    api_response = [
        {
            "file_path": "imap/spice/spin/imap_2025_152_2025_153_01.spin",
            "start_date": "2025-06-01, 00:00:00",
            "end_date": "2025-06-02, 00:00:00",
            "version": "01",
            "ingestion_date": "2025-06-02, 01:50:09",
        },
    ]

    define_spin_table_api_mapping(wiremock_manager, api_response)
    define_spin_file_download_mapping(
        wiremock_manager,
        "imap/spice/spin/imap_2025_152_2025_153_01.spin",
    )

    await execute_pipeline_under_test(wiremock_manager, test_database)

    # Verify the file is in the database with metadata
    files = test_database.get_files_by_path("spice/spin")
    assert len(files) == 1
    assert files[0].file_meta is not None
    assert files[0].file_meta["start_date"] == "2025-06-01, 00:00:00"
    assert files[0].file_meta["end_date"] == "2025-06-02, 00:00:00"
    assert files[0].file_meta["version"] == "01"
    assert files[0].file_meta["ingestion_date"] == "2025-06-02, 01:50:09"
    assert files[0].version == 1


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") and os.getenv("RUNNER_OS") == "Windows",
    reason="Wiremock test containers will not work on Windows Github Runner",
)
@pytest.mark.asyncio
async def test_poll_spin_table_without_database(
    wiremock_manager,
    test_database,  # noqa: F811
    mock_datetime_provider,  # noqa: F811
    dynamic_work_folder,
    clean_datastore,
):
    """Test that the pipeline works without a database."""
    wiremock_manager.reset()

    api_response = [
        {
            "file_path": "imap/spice/spin/imap_2025_152_2025_153_01.spin",
            "start_date": "2025-06-01, 00:00:00",
            "end_date": "2025-06-02, 00:00:00",
            "version": "01",
            "ingestion_date": "2025-06-02, 01:50:09",
        },
    ]

    define_spin_table_api_mapping(wiremock_manager, api_response)
    define_spin_file_download_mapping(
        wiremock_manager,
        "imap/spice/spin/imap_2025_152_2025_153_01.spin",
    )

    await execute_pipeline_under_test(
        wiremock_manager, test_database, use_database=False
    )

    check_spin_file_existence("imap_2025_152_2025_153_01.spin")


async def execute_pipeline_under_test(
    wiremock_manager,
    test_database,  # noqa: F811
    start_date=None,
    end_date=None,
    use_database=True,
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

        pipeline = SpinTablePipeline(
            database=database, settings=settings, client=client
        )
        pipeline.build(run_params)
        await pipeline.run()
