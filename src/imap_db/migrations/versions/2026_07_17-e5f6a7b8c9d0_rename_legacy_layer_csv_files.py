"""Rename layer CSV files still using the legacy _vNNN naming scheme.

Migration ``d4e5f6a7b8c9`` (rename_layer_files_to_major_version) renamed layer
JSON files from the legacy ``_vNNN.json`` naming to the new
``_v001.NNNN.json`` major-version scheme, and was supposed to rename each
JSON's companion CSV file at the same time. It had a bug: it derived the old
CSV filename via ``old_json_name.replace(".json", ".csv")``, but the CSV
files are not named the same as their JSON siblings -- they use a
``-layer-data`` descriptor rather than ``-layer`` (e.g.
``imap_mag_manual-norm-layer-data_20260428_v041.csv``). Because that
computed name never matched a real file, the CSV rename/DB-update was
silently skipped for every layer pair, even though it ran in production.

This migration completes the original intent. For each active layer JSON
file (already renamed to the new ``_v001.NNNN.json`` scheme) it:
  1. Reads ``metadata.data_filename`` from the JSON -- already rewritten to
     the new-style CSV name by the previous migration.
  2. Derives the legacy CSV name by converting the new-style version suffix
     (``_v001.NNNN``) back to the legacy suffix (``_vNNN``).
  3. Looks up the CSV's database record under its legacy name.
  4. Renames the CSV file on disk to the new-style name.
  5. Updates the CSV's ``files`` row (name, path, version_major) to match.

The JSON file and its metadata are not modified -- they were already
corrected by the previous migration.

The downgrade is intentionally a no-op: reverting filename changes on disk
is error-prone and the old naming scheme is considered superseded.

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-07-17 00:00:00.000000

"""

import json
import logging
import os
import re
from pathlib import Path

import sqlalchemy as sa
from alembic import op

from imap_mag import __version__

# revision identifiers, used by Alembic.
revision = "e5f6a7b8c9d0"
down_revision = "d4e5f6a7b8c9"
branch_labels = None
depends_on = None

logger = logging.getLogger(__name__)

_LAYER_JSON_LIKE = "%-layer%.json"
_NEW_CSV_VERSION_RE = re.compile(r"_v001\.(\d+)\.csv$")


def _legacy_csv_name(new_csv_name: str) -> str | None:
    """Convert a new-style ``_v001.NNNN.csv`` CSV name to its legacy ``_vNNN.csv`` name.

    Args:
        new_csv_name: CSV filename using the new major-version naming scheme.

    Returns:
        The equivalent legacy-format CSV filename, or None if new_csv_name
        does not match the expected new-style version suffix.
    """
    match = _NEW_CSV_VERSION_RE.search(new_csv_name)
    if not match:
        return None

    minor = int(match.group(1))
    return new_csv_name[: match.start()] + f"_v{minor:03d}.csv"


def _run_migration(connection: sa.engine.Connection, datastore_path: Path) -> None:
    """Rename legacy layer CSV files that Migration B failed to rename.

    Separated from upgrade() so it can be called directly in tests with an
    arbitrary datastore path and connection.

    Args:
        connection: Active SQLAlchemy connection used for DB updates.
        datastore_path: Root path of the datastore on disk.
    """
    rows = connection.execute(
        sa.text(
            "SELECT id, name, path FROM files "
            "WHERE name LIKE :pattern AND deletion_date IS NULL"
        ),
        {"pattern": _LAYER_JSON_LIKE},
    ).fetchall()

    logger.info(
        f"Found {len(rows)} active layer JSON files to inspect in {datastore_path}."
    )

    for _file_id, json_name, json_path in rows:
        json_disk_path = datastore_path / json_path
        if not json_disk_path.exists():
            logger.warning(f"JSON file {json_disk_path} not found on disk. Skipping.")
            continue

        try:
            with open(json_disk_path) as f:
                data = json.load(f)
        except Exception as e:
            logger.warning(f"Could not read JSON {json_name}: {e}. Skipping.")
            continue

        new_csv_name = (
            data.get("metadata", {}).get("data_filename")
            if isinstance(data, dict)
            else None
        )
        if not new_csv_name:
            logger.debug(f"No metadata.data_filename found in {json_name}; skipping.")
            continue

        # --- already renamed? ---
        already_done = connection.execute(
            sa.text(
                "SELECT id FROM files WHERE name = :name AND deletion_date IS NULL"
            ),
            {"name": new_csv_name},
        ).fetchone()
        if already_done is not None:
            logger.debug(f"CSV {new_csv_name} already renamed. Skipping.")
            continue

        old_csv_name = _legacy_csv_name(new_csv_name)
        if old_csv_name is None:
            logger.warning(
                f"Could not derive legacy CSV name from {new_csv_name!r} "
                f"(referenced by {json_name}). Skipping."
            )
            continue

        # --- look up legacy CSV row in DB ---
        csv_row = connection.execute(
            sa.text(
                "SELECT id, path FROM files "
                "WHERE name = :csv_name AND deletion_date IS NULL"
            ),
            {"csv_name": old_csv_name},
        ).fetchone()

        if csv_row is None:
            logger.warning(
                f"No DB record found for legacy CSV {old_csv_name!r} "
                f"(referenced by {json_name}). Skipping."
            )
            continue

        new_csv_path = csv_row.path.replace(old_csv_name, new_csv_name)

        # --- rename CSV file on disk ---
        old_csv_disk_path = datastore_path / csv_row.path
        new_csv_disk_path = datastore_path / new_csv_path

        if old_csv_disk_path.exists():
            try:
                new_csv_disk_path.parent.mkdir(parents=True, exist_ok=True)
                old_csv_disk_path.rename(new_csv_disk_path)
                logger.info(f"Renamed CSV: {old_csv_name} -> {new_csv_name}")
            except OSError as e:
                logger.error(
                    f"Failed to rename {old_csv_disk_path}: {e}. Skipping row."
                )
                continue
        else:
            logger.warning(
                f"CSV file {old_csv_disk_path} not found on disk; updating DB only."
            )

        # --- update DB: CSV row ---
        connection.execute(
            sa.text(
                "UPDATE files "
                "SET name = :name, path = :path, version_major = 1, "
                "last_modified_date = CURRENT_TIMESTAMP, software_version = :software_version "
                "WHERE id = :id"
            ),
            {
                "name": new_csv_name,
                "path": new_csv_path,
                "id": csv_row.id,
                "software_version": __version__,
            },
        )
        logger.info(f"Updated DB record for CSV {old_csv_name} -> {new_csv_name}.")


def upgrade() -> None:
    """Run the legacy layer CSV rename migration."""
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
            "Skipping legacy layer CSV rename migration."
        )
        return

    _run_migration(op.get_bind(), datastore)


def downgrade() -> None:
    pass  # Intentional no-op: reverting on-disk renames is not supported.
