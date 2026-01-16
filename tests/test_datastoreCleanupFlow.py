from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from imap_db.model import File
from imap_mag.config.DatastoreCleanupConfig import CleanupMode, CleanupTask
from prefect_server.datastoreCleanupFlow import (
    _get_files_to_cleanup,
    _identify_non_latest_versions,
    cleanup_datastore,
)
from tests.util.miscellaneous import create_test_file
from tests.util.prefect import prefect_test_fixture  # noqa: F401


class TestIdentifyNonLatestVersions:
    """Test the non-latest version identification function."""

    def test_identifies_non_latest_versions(self):
        files = [
            _create_mock_file("imap_mag_l1_hsk-procstat_20251101_v001.csv", 1),
            _create_mock_file("imap_mag_l1_hsk-procstat_20251101_v002.csv", 2),
            _create_mock_file("imap_mag_l1_hsk-procstat_20251101_v003.csv", 3),
        ]

        non_latest = _identify_non_latest_versions(files)

        assert len(non_latest) == 2
        paths = [f.path for f in non_latest]
        assert "imap_mag_l1_hsk-procstat_20251101_v001.csv" in paths
        assert "imap_mag_l1_hsk-procstat_20251101_v002.csv" in paths

    def test_returns_empty_for_single_file(self):
        files = [
            _create_mock_file("imap_mag_l1_hsk-procstat_20251101_v001.csv", 1),
        ]

        non_latest = _identify_non_latest_versions(files)

        assert len(non_latest) == 0

    def test_groups_by_file_type_and_date(self):
        files = [
            _create_mock_file("imap_mag_l1_hsk-procstat_20251101_v001.csv", 1),
            _create_mock_file("imap_mag_l1_hsk-procstat_20251101_v002.csv", 2),
            _create_mock_file("imap_mag_l1_hsk-status_20251101_v001.csv", 3),
            _create_mock_file("imap_mag_l1_hsk-status_20251101_v002.csv", 4),
        ]

        non_latest = _identify_non_latest_versions(files)

        assert len(non_latest) == 2
        paths = [f.path for f in non_latest]
        assert "imap_mag_l1_hsk-procstat_20251101_v001.csv" in paths
        assert "imap_mag_l1_hsk-status_20251101_v001.csv" in paths


class TestGetFilesToCleanup:
    """Test the file selection logic."""

    def test_filters_by_age(self):
        files = [
            _create_mock_file("hk/mag/l1/old_v001.csv", 1, days_old=60),
            _create_mock_file("hk/mag/l1/new_v001.csv", 2, days_old=5),
        ]

        task = CleanupTask(
            name="test",
            paths_to_match=["hk/mag/l1/*"],
            files_older_than="30d",
            keep_latest_version_only=False,
            cleanup_mode=CleanupMode.DELETE,
        )

        age_cutoff = datetime.now(tz=UTC) - timedelta(days=30)
        result = _get_files_to_cleanup(files, task, age_cutoff)

        assert len(result) == 1
        assert result[0].path == "hk/mag/l1/old_v001.csv"

    def test_keeps_latest_version_only(self):
        files = [
            _create_mock_file("hk/mag/l1/file_20251101_v001.csv", 1, days_old=60),
            _create_mock_file("hk/mag/l1/file_20251101_v002.csv", 2, days_old=60),
        ]

        task = CleanupTask(
            name="test",
            paths_to_match=["hk/mag/l1/*"],
            files_older_than="30d",
            keep_latest_version_only=True,
            cleanup_mode=CleanupMode.DELETE,
        )

        age_cutoff = datetime.now(tz=UTC) - timedelta(days=30)
        result = _get_files_to_cleanup(files, task, age_cutoff)

        assert len(result) == 1
        assert "v001" in result[0].path

    def test_removes_all_when_keep_latest_false(self):
        files = [
            _create_mock_file("hk/mag/l1/file_20251101_v001.csv", 1, days_old=60),
            _create_mock_file("hk/mag/l1/file_20251101_v002.csv", 2, days_old=60),
        ]

        task = CleanupTask(
            name="test",
            paths_to_match=["hk/mag/l1/*"],
            files_older_than="30d",
            keep_latest_version_only=False,
            cleanup_mode=CleanupMode.DELETE,
        )

        age_cutoff = datetime.now(tz=UTC) - timedelta(days=30)
        result = _get_files_to_cleanup(files, task, age_cutoff)

        assert len(result) == 2


class TestCleanupTaskValidation:
    """Test CleanupTask validation."""

    def test_archive_mode_requires_archive_folder(self):
        with pytest.raises(ValueError, match="archive_folder is required"):
            CleanupTask(
                name="test",
                paths_to_match=["*"],
                cleanup_mode=CleanupMode.ARCHIVE,
                archive_folder=None,
            )

    def test_archive_mode_with_folder_succeeds(self):
        task = CleanupTask(
            name="test",
            paths_to_match=["*"],
            cleanup_mode=CleanupMode.ARCHIVE,
            archive_folder=Path("/archive"),
        )
        assert task.cleanup_mode == CleanupMode.ARCHIVE

    def test_delete_mode_without_archive_folder_succeeds(self):
        task = CleanupTask(
            name="test",
            paths_to_match=["*"],
            cleanup_mode=CleanupMode.DELETE,
        )
        assert task.cleanup_mode == CleanupMode.DELETE


def _create_mock_file(path: str, file_id: int, days_old: int = 0) -> File:
    """Create a mock File object for testing."""
    modified_date = datetime.now(tz=UTC) - timedelta(days=days_old)
    file = File(
        id=file_id,
        name=Path(path).name,
        path=path,
        version=1,
        hash="test-hash",
        size=100,
        creation_date=modified_date,
        last_modified_date=modified_date,
        software_version="1.0.0",
    )
    return file


@pytest.mark.asyncio
async def test_cleanup_datastore_dry_run(
    capture_cli_logs,
    test_database,
    temp_datastore,
    prefect_test_fixture,  # noqa: F811
):
    """Test that dry run mode logs files without removing them."""
    test_files_info = [
        (
            "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv",
            1,
        ),
        (
            "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v002.csv",
            2,
        ),
    ]

    old_date = datetime.now(tz=UTC) - timedelta(days=60)

    for file_path_str, version in test_files_info:
        file_path = temp_datastore / file_path_str
        create_test_file(file_path, "test content")

        file = File(
            name=Path(file_path_str).name,
            path=file_path_str,
            version=version,
            hash="test-hash",
            size=100,
            creation_date=old_date,
            last_modified_date=old_date,
            software_version="1.0.0",
        )
        test_database.insert_file(file)

    await cleanup_datastore(
        task_names=["hk-old-versions"],
        dry_run=True,
    )

    assert "[DRY RUN]" in capture_cli_logs.text
    assert "v001" in capture_cli_logs.text

    # File still exists
    assert (
        temp_datastore
        / "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv"
    ).exists()


@pytest.mark.asyncio
async def test_cleanup_datastore_deletes_non_latest(
    capture_cli_logs,
    test_database,
    temp_datastore,
    prefect_test_fixture,  # noqa: F811
):
    """Test that cleanup deletes non-latest files."""
    test_files_info = [
        (
            "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv",
            1,
        ),
        (
            "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v002.csv",
            2,
        ),
    ]

    old_date = datetime.now(tz=UTC) - timedelta(days=60)

    for file_path_str, version in test_files_info:
        file_path = temp_datastore / file_path_str
        create_test_file(file_path, "test content")

        file = File(
            name=Path(file_path_str).name,
            path=file_path_str,
            version=version,
            hash="test-hash",
            size=100,
            creation_date=old_date,
            last_modified_date=old_date,
            software_version="1.0.0",
        )
        test_database.insert_file(file)

    await cleanup_datastore(
        task_names=["hk-old-versions"],
        dry_run=False,
    )

    assert "Deleted" in capture_cli_logs.text

    # v001 deleted, v002 still exists
    assert not (
        temp_datastore
        / "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv"
    ).exists()
    assert (
        temp_datastore
        / "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v002.csv"
    ).exists()


@pytest.mark.asyncio
async def test_cleanup_datastore_respects_min_age(
    capture_cli_logs,
    test_database,
    temp_datastore,
    prefect_test_fixture,  # noqa: F811
):
    """Test that files younger than min_age are not removed."""
    test_files_info = [
        (
            "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv",
            1,
        ),
        (
            "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v002.csv",
            2,
        ),
    ]

    recent_date = datetime.now(tz=UTC) - timedelta(days=5)

    for file_path_str, version in test_files_info:
        file_path = temp_datastore / file_path_str
        create_test_file(file_path, "test content")

        file = File(
            name=Path(file_path_str).name,
            path=file_path_str,
            version=version,
            hash="test-hash",
            size=100,
            creation_date=recent_date,
            last_modified_date=recent_date,
            software_version="1.0.0",
        )
        test_database.insert_file(file)

    await cleanup_datastore(
        task_names=["hk-old-versions"],
        dry_run=False,
    )

    assert "No files to clean up" in capture_cli_logs.text

    # Both files still exist
    assert (
        temp_datastore
        / "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv"
    ).exists()
    assert (
        temp_datastore
        / "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v002.csv"
    ).exists()


@pytest.mark.asyncio
async def test_cleanup_datastore_archives_files(
    capture_cli_logs,
    test_database,
    temp_datastore,
    tmp_path,
    prefect_test_fixture,  # noqa: F811
    mocker,
):
    """Test that files can be archived instead of deleted."""
    archive_folder = tmp_path / "archive"
    archive_folder.mkdir()

    test_files_info = [
        (
            "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv",
            1,
        ),
        (
            "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v002.csv",
            2,
        ),
    ]

    old_date = datetime.now(tz=UTC) - timedelta(days=60)

    for file_path_str, version in test_files_info:
        file_path = temp_datastore / file_path_str
        create_test_file(file_path, "test content")

        file = File(
            name=Path(file_path_str).name,
            path=file_path_str,
            version=version,
            hash="test-hash",
            size=100,
            creation_date=old_date,
            last_modified_date=old_date,
            software_version="1.0.0",
        )
        test_database.insert_file(file)

    # Mock the config to use archive mode
    from imap_mag.config.AppSettings import AppSettings
    from imap_mag.config.DatastoreCleanupConfig import DatastoreCleanupConfig

    mock_config = DatastoreCleanupConfig(
        dry_run=False,
        tasks=[
            CleanupTask(
                name="archive-test",
                paths_to_match=["hk/mag/l1/hsk-procstat/*/*.csv"],
                files_older_than="30d",
                keep_latest_version_only=True,
                cleanup_mode=CleanupMode.ARCHIVE,
                archive_folder=archive_folder,
            )
        ],
    )

    original_init = AppSettings.__init__

    def patched_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self.datastore_cleanup = mock_config

    mocker.patch.object(AppSettings, "__init__", patched_init)

    await cleanup_datastore(
        task_names=["archive-test"],
        dry_run=False,
    )

    assert "Archived" in capture_cli_logs.text

    # Original v001 should be gone
    assert not (
        temp_datastore
        / "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv"
    ).exists()

    # v001 should be in archive
    assert (
        archive_folder
        / "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv"
    ).exists()

    # v002 still in original location
    assert (
        temp_datastore
        / "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v002.csv"
    ).exists()


@pytest.mark.asyncio
async def test_cleanup_datastore_no_matching_files(
    capture_cli_logs,
    test_database,
    temp_datastore,
    prefect_test_fixture,  # noqa: F811
):
    """Test flow handles case when no files match patterns."""
    # Don't add any files to database

    await cleanup_datastore(
        task_names=["hk-old-versions"],
        dry_run=False,
    )

    assert "No files match patterns" in capture_cli_logs.text


@pytest.mark.asyncio
async def test_cleanup_datastore_unknown_task(
    capture_cli_logs,
    test_database,
    temp_datastore,
    prefect_test_fixture,  # noqa: F811
):
    """Test flow handles unknown task names gracefully."""
    await cleanup_datastore(
        task_names=["nonexistent-task"],
        dry_run=False,
    )

    assert "No tasks found matching" in capture_cli_logs.text


@pytest.mark.asyncio
async def test_cleanup_datastore_max_file_operations(
    capture_cli_logs,
    test_database,
    temp_datastore,
    prefect_test_fixture,  # noqa: F811
):
    """Test that max_file_operations limit is respected."""
    # Create 5 files
    test_files_info = [
        (
            f"hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v00{i}.csv",
            i,
        )
        for i in range(1, 6)
    ]

    old_date = datetime.now(tz=UTC) - timedelta(days=60)

    for file_path_str, version in test_files_info:
        file_path = temp_datastore / file_path_str
        create_test_file(file_path, "test content")

        file = File(
            name=Path(file_path_str).name,
            path=file_path_str,
            version=version,
            hash="test-hash",
            size=100,
            creation_date=old_date,
            last_modified_date=old_date,
            software_version="1.0.0",
        )
        test_database.insert_file(file)

    # Only allow 2 operations
    await cleanup_datastore(
        task_names=["hk-old-versions"],
        dry_run=True,
        max_file_operations=2,
    )

    assert "stopped at limit (2)" in capture_cli_logs.text
    # Should only log 2 files
    assert capture_cli_logs.text.count("[DRY RUN] Would delete") == 2
