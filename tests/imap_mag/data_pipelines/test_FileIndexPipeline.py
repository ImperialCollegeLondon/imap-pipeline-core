"""Integration tests for FileIndexPipeline and GetFilesToIndexStage."""

from datetime import UTC, datetime, timedelta

from imap_db.model import File
from imap_mag.data_pipelines import (
    AutomaticRunParameters,
    IndexByDateRangeRunParameters,
    IndexByFileNamesRunParameters,
    IndexByIdsRunParameters,
)
from imap_mag.data_pipelines.FileIndexPipeline import FileIndexPipeline
from imap_mag.util.Environment import Environment
from tests.util.miscellaneous import DATASTORE


def _insert_file(
    test_database, file_path_str: str, app_settings, modified_offset_seconds: int = 1
) -> File:
    """Insert a test file into the tracking database and return the File object with its DB id."""
    file_path = DATASTORE / file_path_str
    stem = file_path.stem
    version_str = stem.split("_v")[-1]
    version = int(version_str)
    date_str = stem.split("_")[-2]
    content_date = datetime(
        int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]), tzinfo=UTC
    )
    file = File.from_file(file_path, version, "HASH", content_date, app_settings)
    file.last_modified_date = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(
        seconds=modified_offset_seconds
    )
    test_database.insert_file(file)
    return test_database.get_files(File.path == file_path_str)[0]


def _build_and_run_pipeline(db, settings, run_params):
    import asyncio

    pipeline = FileIndexPipeline(database=db, settings=settings)
    pipeline.build(run_parameters=run_params)
    asyncio.get_event_loop().run_until_complete(pipeline.run())
    return pipeline


# ---------------------------------------------------------------------------
# IndexByIdsRunParameters
# ---------------------------------------------------------------------------


def test_index_by_ids_indexes_specified_file(test_database, test_database_container):
    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings()  # type: ignore
        file = _insert_file(
            test_database,
            "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf",
            settings,
        )

        _build_and_run_pipeline(
            test_database, settings, IndexByIdsRunParameters(file_ids=[file.id])
        )

        idx = test_database.get_file_index_by_file_id(file.id)
        assert idx is not None
        assert idx.record_count == 100


def test_index_by_ids_skips_deleted_file(test_database, test_database_container):
    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings()  # type: ignore
        file = _insert_file(
            test_database,
            "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf",
            settings,
        )
        file.set_deleted()
        test_database.save(file)

        _build_and_run_pipeline(
            test_database, settings, IndexByIdsRunParameters(file_ids=[file.id])
        )

        idx = test_database.get_file_index_by_file_id(file.id)
        assert idx is None


def test_index_by_ids_upserts_on_second_run(test_database, test_database_container):
    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings()  # type: ignore
        file = _insert_file(
            test_database,
            "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf",
            settings,
        )
        params = IndexByIdsRunParameters(file_ids=[file.id])

        _build_and_run_pipeline(test_database, settings, params)
        _build_and_run_pipeline(test_database, settings, params)

        idx = test_database.get_file_index_by_file_id(file.id)
        assert idx is not None
        assert idx.record_count == 100


# ---------------------------------------------------------------------------
# IndexByDateRangeRunParameters
# ---------------------------------------------------------------------------


def test_index_by_date_range_finds_files_in_range(
    test_database, test_database_container
):
    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings()  # type: ignore
        # Insert two files at slightly different modification timestamps
        f1 = _insert_file(
            test_database,
            "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf",
            settings,
            modified_offset_seconds=10,
        )
        f2 = _insert_file(
            test_database,
            "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv",
            settings,
            modified_offset_seconds=20,
        )

        after = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=5)
        params = IndexByDateRangeRunParameters(modified_after=after)
        _build_and_run_pipeline(test_database, settings, params)

        idx1 = test_database.get_file_index_by_file_id(f1.id)
        idx2 = test_database.get_file_index_by_file_id(f2.id)
        assert idx1 is not None
        assert idx2 is not None


def test_index_by_date_range_excludes_files_before_range(
    test_database, test_database_container
):
    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings()  # type: ignore
        file = _insert_file(
            test_database,
            "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf",
            settings,
            modified_offset_seconds=5,
        )

        # Set after > file modification time → should not be picked up
        after = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=100)
        params = IndexByDateRangeRunParameters(modified_after=after)
        _build_and_run_pipeline(test_database, settings, params)

        idx = test_database.get_file_index_by_file_id(file.id)
        assert idx is None


# ---------------------------------------------------------------------------
# IndexByFileNamesRunParameters
# ---------------------------------------------------------------------------


def test_index_by_file_names_matches_exact_path(test_database, test_database_container):
    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings()  # type: ignore
        rel_path = "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
        file = _insert_file(test_database, rel_path, settings)

        params = IndexByFileNamesRunParameters(file_paths=[rel_path])
        _build_and_run_pipeline(test_database, settings, params)

        idx = test_database.get_file_index_by_file_id(file.id)
        assert idx is not None
        assert idx.record_count == 100


def test_index_by_file_names_glob_pattern(test_database, test_database_container):
    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings()  # type: ignore
        rel_path = "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
        file = _insert_file(test_database, rel_path, settings)

        params = IndexByFileNamesRunParameters(file_paths=["science/mag/l1c/**/*.cdf"])
        _build_and_run_pipeline(test_database, settings, params)

        idx = test_database.get_file_index_by_file_id(file.id)
        assert idx is not None


# ---------------------------------------------------------------------------
# AutomaticRunParameters (uses workflow progress)
# ---------------------------------------------------------------------------


def test_automatic_mode_indexes_newly_modified_files(
    test_database, test_database_container
):
    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings()  # type: ignore
        rel_path = "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
        file = _insert_file(
            test_database, rel_path, settings, modified_offset_seconds=10
        )

        # Progress is before the file modification time → should pick it up
        params = AutomaticRunParameters()
        _build_and_run_pipeline(test_database, settings, params)

        idx = test_database.get_file_index_by_file_id(file.id)
        assert idx is not None


# ---------------------------------------------------------------------------
# CSV bad data detection at pipeline level
# ---------------------------------------------------------------------------


def test_pipeline_indexes_cdf_bad_data(test_database, test_database_container):
    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings()  # type: ignore
        rel_path = "science/mag/l1d/2026/02/imap_mag_l1d_norm-gse_20260222_v001.cdf"
        file = _insert_file(test_database, rel_path, settings)

        params = IndexByIdsRunParameters(file_ids=[file.id])
        _build_and_run_pipeline(test_database, settings, params)

        idx = test_database.get_file_index_by_file_id(file.id)
        assert idx is not None
        assert idx.has_bad_data is True
        assert idx.record_count == 172800


# ---------------------------------------------------------------------------
# Workflow progress tracking
# ---------------------------------------------------------------------------


def test_automatic_mode_updates_workflow_progress_after_run(
    test_database, test_database_container
):
    """After an automatic run, workflow progress timestamp must advance so the same
    files are NOT re-indexed on the next automatic run."""
    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings()  # type: ignore
        rel_path = "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
        file = _insert_file(
            test_database, rel_path, settings, modified_offset_seconds=10
        )

        # First automatic run
        _build_and_run_pipeline(test_database, settings, AutomaticRunParameters())

        progress = test_database.get_workflow_progress(
            FileIndexPipeline.PROGRESS_ITEM_ID
        )
        assert progress.progress_timestamp is not None
        assert progress.progress_timestamp >= file.last_modified_date

        # Second automatic run should find nothing new
        _build_and_run_pipeline(test_database, settings, AutomaticRunParameters())

        # File should still have exactly one index entry (no duplicate)
        idx = test_database.get_file_index_by_file_id(file.id)
        assert idx is not None


def test_manual_run_does_not_update_workflow_progress(
    test_database, test_database_container
):
    """Manual runs (IndexByIds) must NOT advance workflow progress so automatic
    scheduling is unaffected."""
    with Environment(MAG_DATA_STORE=str(DATASTORE.absolute())):
        from imap_mag.config.AppSettings import AppSettings

        settings = AppSettings()  # type: ignore
        rel_path = "science/mag/l1c/2025/04/imap_mag_l1c_norm-mago_20250421_v001.cdf"
        file = _insert_file(test_database, rel_path, settings)

        progress_before = test_database.get_workflow_progress(
            FileIndexPipeline.PROGRESS_ITEM_ID
        )
        ts_before = progress_before.progress_timestamp

        _build_and_run_pipeline(
            test_database, settings, IndexByIdsRunParameters(file_ids=[file.id])
        )

        progress_after = test_database.get_workflow_progress(
            FileIndexPipeline.PROGRESS_ITEM_ID
        )
        assert progress_after.progress_timestamp == ts_before
