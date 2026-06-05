"""Tests for the datastoreIndexerFlow."""

import pytest

from imap_db.model import File
from prefect_server.datastoreIndexerFlow import index_datastore_flow
from tests.util.miscellaneous import create_test_file
from tests.util.prefect_test_utils import prefect_test_fixture  # noqa: F401


def _make_hk_file(temp_datastore, rel_path: str, content: str = "test") -> None:
    """Create a real HK CSV file in the temp datastore."""
    create_test_file(temp_datastore / rel_path, content)


def _db_record_for_path(test_database, rel_path: str) -> File | None:
    """Return the first database record matching *rel_path* (any deletion status)."""
    records = test_database.get_files(File.path == rel_path)
    return records[0] if records else None


@pytest.mark.asyncio
async def test_index_datastore_skips_already_indexed_file(
    test_database,
    temp_datastore,
):
    """File on disk and already active in DB should be skipped without changes."""
    rel_path = (
        "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv"
    )
    full_path = temp_datastore / rel_path
    _make_hk_file(temp_datastore, rel_path)

    # Pre-insert an active record so the file is already indexed
    existing = File(
        name=full_path.name,
        path=rel_path,
        descriptor=File.get_descriptor_from_filename(full_path.name),
        version=1,
        hash="existing-hash",
        size=full_path.stat().st_size,
        content_date=None,
        software_version="1.0.0",
    )
    test_database.upsert_file(existing)

    await index_datastore_flow.fn()

    # Record should be unchanged (hash not overwritten)
    record = _db_record_for_path(test_database, rel_path)
    assert record is not None
    assert record.hash == "existing-hash"
    assert record.deletion_date is None


@pytest.mark.asyncio
async def test_index_datastore_indexes_new_file(
    capture_cli_logs, test_database, temp_datastore
):
    """File on disk with no DB record should be inserted into the database."""
    rel_path = (
        "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv"
    )
    _make_hk_file(temp_datastore, rel_path, content="brand-new-content")

    # Confirm there is no existing record
    assert _db_record_for_path(test_database, rel_path) is None

    await index_datastore_flow.fn()

    record = _db_record_for_path(test_database, rel_path)
    assert record is not None
    assert record.deletion_date is None
    assert "indexed" in capture_cli_logs.text


@pytest.mark.asyncio
async def test_index_datastore_restores_soft_deleted_file(
    capture_cli_logs, test_database, temp_datastore
):
    """File on disk whose DB record has a deletion_date should have it cleared."""
    rel_path = (
        "hk/mag/l1/hsk-procstat/2025/11/imap_mag_l1_hsk-procstat_20251101_v001.csv"
    )
    full_path = temp_datastore / rel_path
    _make_hk_file(temp_datastore, rel_path)

    # Insert a soft-deleted record for the file
    deleted_record = File(
        name=full_path.name,
        path=rel_path,
        descriptor=File.get_descriptor_from_filename(full_path.name),
        version=1,
        hash="deleted-hash",
        size=full_path.stat().st_size,
        content_date=None,
        software_version="1.0.0",
    )
    test_database.upsert_file(deleted_record)

    # Mark it as deleted
    record = _db_record_for_path(test_database, rel_path)
    assert record is not None
    record.set_deleted()
    test_database.save(record)

    deletion_ts = record.deletion_date
    assert deletion_ts is not None

    await index_datastore_flow.fn()

    restored = _db_record_for_path(test_database, rel_path)
    assert restored is not None
    assert restored.deletion_date is None, (
        "deletion_date should be cleared after indexing a soft-deleted file"
    )
    assert "restored" in capture_cli_logs.text


@pytest.mark.asyncio
async def test_index_datastore_empty_datastore(
    test_database,
    clean_datastore,
):
    """A datastore containing only files without recognised handlers is handled gracefully."""
    create_test_file(clean_datastore / "unknown_file.xyz", "irrelevant")

    await index_datastore_flow.fn()

    # Should complete without errors; no files indexed
    records = test_database.get_files()
    assert len(records) == 0
