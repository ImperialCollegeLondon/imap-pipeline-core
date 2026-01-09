from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

from imap_db.model import File
from imap_mag.config.AppSettings import AppSettings
from imap_mag.util import Environment
from prefect_server.postgresUploadFlow import upload_new_files_to_postgres
from tests.util.miscellaneous import DATASTORE
from tests.util.prefect import prefect_test_fixture  # noqa: F401


@pytest.mark.asyncio
async def test_upload_new_files_to_postgres_does_upload_files(
    capture_cli_logs,
    test_database,
    prefect_test_fixture,  # noqa: F811
):
    # Set up test data in IMAP files database table
    test_files = [
        "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv",
        "hk/mag/l1/hsk-status/2025/11/imap_mag_l1_hsk-status_20251101_v001.csv",  # not in config file
        "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251102_v001.csv",
        "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251102_v002.csv",
    ]

    # Use the test crump config file
    crump_config_path = Path("tests/test_crump_config.yaml")

    # Set up test environment with a target database for crump to write to
    with PostgresContainer(driver="psycopg") as target_postgres:
        target_db_url = target_postgres.get_connection_url()
        target_engine = create_engine(target_db_url)

        with Environment(
            MAG_DATA_STORE=str(DATASTORE.absolute()),
            MAG_POSTGRES_UPLOAD__CRUMP_CONFIG_PATH=str(crump_config_path.absolute()),
            TARGET_DATABASE_URL=target_db_url,
        ):
            # Insert test files into IMAP tracking database
            app_settings = AppSettings()
            for file_path_str in test_files:
                file_path = DATASTORE / file_path_str
                # Extract version from filename (e.g., v001 -> 1, v002 -> 2)
                version = int(file_path.stem.split("_v")[-1])
                test_database.insert_file(
                    File.from_file(
                        file_path,
                        version,
                        "NOT-REAL-HASH",
                        datetime(2025, 11, 1, tzinfo=UTC),
                        app_settings,
                    )
                )

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
            assert "3 file(s) uploaded to PostgreSQL" in capture_cli_logs.text

            # Verify data was uploaded to target database
            with target_engine.connect() as conn:
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
