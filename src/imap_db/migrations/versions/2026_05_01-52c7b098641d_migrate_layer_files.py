"""Migrate layer files - populate data_hash in JSON metadata and correct DB hash.

For each active layer JSON file tracked in the database this migration:
  1. Reads the JSON from the datastore and locates its companion CSV.
  2. Computes the MD5 hash of the CSV (the canonical content-identity for layer files).
  3. Writes the hash into the JSON metadata (data_hash field) and re-saves the JSON.
  4. Updates the files.hash column in the database to match.

Files that already carry a data_hash are left unchanged unless the DB hash is out of
sync, in which case only the DB record is corrected.

The downgrade is a no-op: adding data_hash is fully backward-compatible.

Revision ID: 52c7b098641d
Revises: 4fdab0d788f0
Create Date: 2026-05-01 10:35:41.546535

"""

import logging
import os
from pathlib import Path

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "52c7b098641d"
down_revision = "4fdab0d788f0"
branch_labels = None
depends_on = None

logger = logging.getLogger(__name__)

# SQL LIKE pattern that matches layer JSON filenames
_LAYER_JSON_LIKE = "%-layer%.json"


def upgrade() -> None:
    datastore: Path | None = None
    try:
        from imap_mag.config.AppSettings import AppSettings

        datastore = AppSettings().data_store  # type: ignore
    except Exception:
        env_val = os.environ.get("MAG_DATA_STORE")
        if env_val:
            datastore = Path(env_val)

    if datastore is None:
        logger.warning(
            "Datastore path not available (set MAG_DATA_STORE or configure AppSettings). "
            "Skipping layer file hash migration."
        )
        return

    _run_migration(op.get_bind(), datastore)


def _run_migration(connection: sa.engine.Connection, datastore: Path) -> None:
    """Backfill data_hash for all active layer JSON files.

    Separated from upgrade() so it can be called directly in tests with an
    arbitrary datastore path and connection.
    """
    from imap_mag.io.file import IFilePathHandler
    from mag_toolkit.calibration.CalibrationLayer import CalibrationLayer

    rows = connection.execute(
        sa.text(
            "SELECT id, name, path, hash FROM files "
            "WHERE name LIKE :pattern AND deletion_date IS NULL"
        ),
        {"pattern": _LAYER_JSON_LIKE},
    ).fetchall()

    logger.info(f"Found {len(rows)} active layer JSON files to migrate.")

    for file_id, name, path, current_hash in rows:
        json_path = datastore / path

        if not json_path.exists():
            logger.warning(f"Layer file {json_path} not found in datastore. Skipping.")
            continue

        try:
            layer = CalibrationLayer.from_file(json_path, load_contents=False)
        except Exception as e:
            logger.warning(f"Could not load layer {json_path}: {e}. Skipping.")
            continue

        if layer.metadata.data_filename is None:
            logger.warning(f"Layer {name} has no data_filename. Skipping.")
            continue

        csv_path = json_path.parent / layer.metadata.data_filename.name
        if not csv_path.exists():
            logger.warning(f"Companion CSV {csv_path} not found. Skipping {name}.")
            continue

        if layer.metadata.data_hash:
            # JSON already has a hash — only fix the DB if it is out of sync
            csv_hash = layer.metadata.data_hash
            logger.debug(f"Layer {name} already has data_hash. Checking DB record.")
        else:
            csv_hash = IFilePathHandler.default_file_hash(csv_path)
            layer.metadata.data_hash = csv_hash
            layer.writeToFile(json_path)
            logger.info(f"Populated data_hash for {name}: {csv_hash}.")

        if current_hash != csv_hash:
            connection.execute(
                sa.text("UPDATE files SET hash = :hash WHERE id = :id"),
                {"hash": csv_hash, "id": file_id},
            )
            logger.info(f"Updated DB hash for {name}: {current_hash!r} → {csv_hash!r}.")


def downgrade() -> None:
    pass  # No-op: data_hash addition is backward-compatible
