from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from imap_db.model import File
from imap_mag.config.AppSettings import AppSettings
from imap_mag.util import Environment
from prefect_server.postgresUploadFlow import upload_new_files_to_postgres
from tests.util.miscellaneous import DATASTORE
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401


@pytest.mark.asyncio
async def test_upload_new_files_to_postgres_does_upload_files(
    capture_cli_logs,
    test_database,
    test_database_container,
    test_database_server_engine,
    prefect_test_fixture,  # noqa: F811
):
    # Set up test data in IMAP files database table
    test_files = [
        "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv",
        "hk/mag/l1/hsk-status/2025/11/imap_mag_l1_hsk-status_20251101_v001.csv",
        "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251102_v001.csv",
        "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251102_v002.csv",
        "hk/mag/l1/prog-mtran/2025/09/imap_mag_l1_prog-mtran_20250927_v002.csv",
        "hk/mag/l1/prog-btsucc/2025/09/imap_mag_l1_prog-btsucc_20250927_v002.csv",
        "hk/sc/l1/x286/2026/02/imap_sc_l1_x286_20260217_v001.csv",
        "hk/sc/l1/x285/2026/02/imap_sc_l1_x285_20260217_v001.csv",
    ]

    # Set up test environment with a target database for crump to write to
    target_db_url = test_database_container.get_connection_url()

    with Environment(
        MAG_DATA_STORE=str(DATASTORE.absolute()),
        TARGET_DATABASE_URL=target_db_url,
    ):
        # Insert test files into IMAP tracking database
        app_settings = AppSettings()
        insert_test_files_into_database(test_database, test_files, app_settings)

        # Exercise - upload files to postgres
        await upload_new_files_to_postgres(
            find_files_after=datetime(2010, 1, 1, tzinfo=UTC),
            db_env_name_or_block_name_or_block="TARGET_DATABASE_URL",
        )

        # Verify
        assert (
            "Synced 1 rows from hk/mag/l1/hsk-status/2025/11/imap_mag_l1_hsk-status_20251101_v001.csv"
            in capture_cli_logs.text
        )
        assert (
            "Synced 24 rows from hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv"
            in capture_cli_logs.text
        )
        assert (
            "Synced 24 rows from hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251102_v002.csv"
            in capture_cli_logs.text
        )

        assert (
            "Synced 31 rows from hk/sc/l1/x285/2026/02/imap_sc_l1_x285_20260217_v001.csv"
            in capture_cli_logs.text
        )

        assert "7 file(s) uploaded to PostgreSQL" in capture_cli_logs.text

        # Verify data was uploaded to target database
        with test_database_server_engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM hsk_procstat"))
            row_count = result.scalar()
            assert row_count == 48, (
                f"Expected rows in hsk_procstat table, but found {row_count}"
            )

        # Exercise again - should not upload again (no new files)
        # Don't pass find_files_after so it uses the updated workflow progress
        await upload_new_files_to_postgres(
            db_env_name_or_block_name_or_block="TARGET_DATABASE_URL",
        )

        # Verify - should be no work to do
        assert "No work to do" in capture_cli_logs.text


@pytest.mark.asyncio
async def test_upload_files_to_postgres_populates_file_date_so_they_can_be_updated_by_later_version(
    capture_cli_logs,
    test_database,
    test_database_server_engine,
    test_database_container,
):
    # Set up test data in IMAP files database table
    test_files = [
        "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv",
        # "hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v002.csv"
    ]

    # Set up test environment with a target database for crump to write to
    target_db_url = test_database_container.get_connection_url()

    with Environment(
        MAG_DATA_STORE=str(DATASTORE.absolute()),
        TARGET_DATABASE_URL=target_db_url,
        PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW="ignore",
    ):
        # Insert test files into IMAP tracking database
        app_settings = AppSettings()
        insert_test_files_into_database(test_database, test_files, app_settings)

        # Exercise - upload files to postgres
        await upload_new_files_to_postgres.fn(
            find_files_after=datetime(2010, 1, 1, tzinfo=UTC),
            db_env_name_or_block_name_or_block="TARGET_DATABASE_URL",
        )

        expected_rows = 5
        # Verify
        assert (
            f"Synced {expected_rows} rows from hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v001.csv"
            in capture_cli_logs.text
        )

        # Verify data was uploaded to target database
        with test_database_server_engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM hsk_pw"))
            row_count = result.scalar()
            assert row_count == expected_rows, (
                f"Expected rows in hsk_procstat table, but found {row_count}"
            )

            result2 = conn.scalar(text("SELECT file_date FROM hsk_pw LIMIT 1"))
            print(f"file_date value: {result2}")
            assert result2 is not None, (
                "Expected file_date to be populated, but it was None"
            )
            assert result2 == datetime(2025, 11, 2, tzinfo=UTC).date(), (
                f"Expected file_date to be 2025-11-02, but got {result2}"
            )

        # second version of the packet csv file
        test_files = ["hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v002.csv"]
        insert_test_files_into_database(test_database, test_files, app_settings)

        # Exercise - upload files to postgres
        await upload_new_files_to_postgres.fn(
            find_files_after=datetime(2010, 1, 1, tzinfo=UTC),
            db_env_name_or_block_name_or_block="TARGET_DATABASE_URL",
        )

        expected_rows = 8
        # Verify
        assert (
            f"Synced {expected_rows} rows from hk/mag/l1/hsk-pw/2025/11/imap_mag_l1_hsk-pw_20251102_v002.csv"
            in capture_cli_logs.text
        )


@pytest.mark.asyncio
async def test_upload_new_files_to_postgres_allows_empty_noaa_wind_values(
    capture_cli_logs,
    test_database,
    test_database_server_engine,
    test_database_container,
    tmp_path,
):
    test_file = tmp_path / "noaa/2026/09/SOLAR1_wind_noaa_20260915.csv"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text(
        "\n".join(
            [
                "time_tag,density,speed,temperature",
                "2026-09-15T03:46:00,0.76,358.2,256915",
                "2026-09-15T03:48:00,,,",
                "2026-09-15T03:49:00,0.79,366.5,278443",
            ]
        )
        + "\n"
    )

    target_db_url = test_database_container.get_connection_url()

    with Environment(
        MAG_DATA_STORE=str(tmp_path),
        TARGET_DATABASE_URL=target_db_url,
        PREFECT_LOGGING_TO_API_WHEN_MISSING_FLOW="ignore",
    ):
        app_settings = AppSettings()
        insert_test_files_into_database(
            test_database,
            ["noaa/2026/09/SOLAR1_wind_noaa_20260915.csv"],
            app_settings,
            datastore_root=tmp_path,
        )

        await upload_new_files_to_postgres.fn(
            find_files_after=datetime(2010, 1, 1, tzinfo=UTC),
            db_env_name_or_block_name_or_block="TARGET_DATABASE_URL",
        )

        assert (
            "Synced 3 rows from noaa/2026/09/SOLAR1_wind_noaa_20260915.csv"
            in capture_cli_logs.text
        )

        with test_database_server_engine.connect() as conn:
            row_count = conn.scalar(text("SELECT COUNT(*) FROM solar_wind_noaa"))
            assert row_count == 3, (
                f"Expected 3 rows in solar_wind_noaa, but found {row_count}"
            )

            null_row_count = conn.scalar(
                text(
                    "SELECT COUNT(*) FROM solar_wind_noaa WHERE density IS NULL AND speed IS NULL AND temperature IS NULL"
                )
            )
            assert null_row_count == 1, (
                "Expected one NOAA wind row with null density, speed, and temperature"
            )


def insert_test_files_into_database(
    test_database, test_files, app_settings, datastore_root=DATASTORE
):
    def is_yyyymmdd_date_part(part: str) -> bool:
        if len(part) != 8 or not part.isdigit():
            return False
        try:
            datetime.strptime(part, "%Y%m%d")
        except ValueError:
            return False
        return True

    last_modified_date = datetime(2026, 1, 1, tzinfo=UTC)
    for file_path_str in test_files:
        file_path = datastore_root / file_path_str
        # Extract version from filename (e.g., v001 -> 1, v002 -> 2)
        version = int(file_path.stem.split("_v")[-1]) if "_v" in file_path.stem else 1
        # Extract date from filename (e.g., 20251101 -> 2025-11-01)
        date_str = next(
            (
                part
                for part in reversed(file_path.stem.split("_"))
                if is_yyyymmdd_date_part(part)
            ),
            None,
        )
        if date_str is None:
            raise ValueError(
                f"Could not find YYYYMMDD date in filename {file_path.name}"
            )
        content_date = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=UTC)
        last_modified_date += timedelta(seconds=1)
        file = File.from_file(
            file_path,
            version,
            "NOT-REAL-HASH",
            content_date,
            app_settings,
        )
        file.last_modified_date = last_modified_date
        test_database.upsert_file(file)
