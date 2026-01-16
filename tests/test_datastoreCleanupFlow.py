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
from prefect_server.durationUtils import format_duration, parse_duration
from prefect_server.fileVersionUtils import extract_version_and_date
from tests.util.miscellaneous import create_test_file
from tests.util.prefect import prefect_test_fixture  # noqa: F401


class TestParseDuration:
    """Test the duration parsing utility."""

    def test_parses_days(self):
        assert parse_duration("30d") == timedelta(days=30)
        assert parse_duration("1D") == timedelta(days=1)

    def test_parses_hours(self):
        assert parse_duration("12h") == timedelta(hours=12)
        assert parse_duration("24H") == timedelta(hours=24)

    def test_parses_minutes(self):
        assert parse_duration("45m") == timedelta(minutes=45)
        assert parse_duration("60M") == timedelta(minutes=60)

    def test_parses_seconds(self):
        assert parse_duration("30s") == timedelta(seconds=30)
        assert parse_duration("60S") == timedelta(seconds=60)

    def test_parses_combinations(self):
        assert parse_duration("1d12h") == timedelta(days=1, hours=12)
        assert parse_duration("2d6h30m") == timedelta(days=2, hours=6, minutes=30)

    def test_raises_on_invalid_format(self):
        with pytest.raises(ValueError):
            parse_duration("invalid")
        with pytest.raises(ValueError):
            parse_duration("")
        with pytest.raises(ValueError):
            parse_duration("30")


class TestFormatDuration:
    """Test the duration formatting utility."""

    def test_formats_days(self):
        assert format_duration(timedelta(days=30)) == "30d"

    def test_formats_hours(self):
        assert format_duration(timedelta(hours=12)) == "12h"

    def test_formats_combinations(self):
        assert format_duration(timedelta(days=1, hours=12)) == "1d12h"

    def test_formats_zero(self):
        assert format_duration(timedelta(0)) == "0s"


class TestExtractVersionAndDate:
    """Test the version and date extraction function."""

    def test_extracts_date_from_yyyymmdd_format(self):
        file_path = Path("imap_mag_l1_hsk-procstat_20251101_v001.csv")
        date, version = extract_version_and_date(file_path)
        assert date == datetime(2025, 11, 1, tzinfo=UTC)
        assert version == 1

    def test_extracts_date_from_yyyy_mm_dd_format(self):
        file_path = Path("some_file_2025-11-01_v002.csv")
        date, version = extract_version_and_date(file_path)
        assert date == datetime(2025, 11, 1, tzinfo=UTC)
        assert version == 2

    def test_extracts_version_from_v_pattern(self):
        file_path = Path("imap_mag_l1_hsk-procstat_20251101_v003.csv")
        _date, version = extract_version_and_date(file_path)
        assert version == 3

    def test_returns_zero_version_when_not_found(self):
        file_path = Path("some_file_without_version.csv")
        _date, version = extract_version_and_date(file_path)
        assert version == 0

    def test_returns_none_date_when_not_found(self):
        file_path = Path("some_file_without_date.csv")
        date, _version = extract_version_and_date(file_path)
        assert date is None


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
