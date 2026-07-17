"""Rename existing layer files from _vNNN format to _v001.NNNN major-version scheme.

For each active layer JSON file tracked in the database this migration:
  1. Detects files still using the old _vNNN.json naming (e.g. _v003.json).
  2. Renames the JSON and its companion CSV to the new _v001.NNNN.json / _v001.NNNN.csv
     format (e.g. _v001.0003.json / _v001.0003.csv).
  3. Updates the files.name, files.path, and files.version_major columns in the DB.
  4. Rewrites the JSON metadata.data_filename field to reference the new CSV name.

The downgrade is intentionally a no-op: reverting filename changes on disk is
error-prone and the old naming scheme is considered superseded.

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-07-09 00:00:00.000000

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
revision = "d4e5f6a7b8c9"
down_revision = "c3d4e5f6a7b8"
branch_labels = None
depends_on = None

logger = logging.getLogger(__name__)

_LAYER_JSON_LIKE = "%-layer%.json"
_OLD_VERSION_RE = re.compile(r"_v(\d+)\.json$")
_NEW_VERSION_RE = re.compile(r"_v\d+\.\d+\.json$")


def _run_migration(connection: sa.engine.Connection, datastore_path: Path) -> None:
    """Rename layer files from _vNNN to _v001.NNNN naming convention.

    Separated from upgrade() so it can be called directly in tests with an
    arbitrary datastore path and connection.

    Args:
        connection: Active SQLAlchemy connection used for DB updates.
        datastore_path: Root path of the datastore on disk.
    """
    rows = connection.execute(
        sa.text(
            "SELECT id, name, path, version_major FROM files "
            "WHERE name LIKE :pattern AND deletion_date IS NULL"
        ),
        {"pattern": _LAYER_JSON_LIKE},
    ).fetchall()

    logger.info(
        f"Found {len(rows)} active layer JSON files to inspect in {datastore_path}."
    )

    for file_id, old_name, old_path, _version_major in rows:
        # --- already in new format? ---
        if _NEW_VERSION_RE.search(old_name):
            logger.debug(f"Layer {old_name} already uses new version format. Skipping.")
            continue

        # --- parse old version number ---
        match = _OLD_VERSION_RE.search(old_name)
        if not match:
            logger.warning(
                f"Layer {old_name} does not match expected _vNNN.json pattern. Skipping."
            )
            continue

        old_minor = int(match.group(1))
        new_name = old_name[: match.start()] + f"_v001.{old_minor:04d}.json"
        new_path = old_path.replace(old_name, new_name)

        # --- companion CSV names ---
        old_csv_name = old_name.replace(".json", ".csv")
        new_csv_name = new_name.replace(".json", ".csv")
        new_csv_path = new_path.replace(".json", ".csv")

        # --- look up CSV row in DB ---
        csv_row = connection.execute(
            sa.text(
                "SELECT id, path FROM files "
                "WHERE name = :csv_name AND deletion_date IS NULL"
            ),
            {"csv_name": old_csv_name},
        ).fetchone()

        # --- rename files on disk ---
        json_disk_path = datastore_path / old_path
        new_json_disk_path = datastore_path / new_path

        if json_disk_path.exists():
            try:
                new_json_disk_path.parent.mkdir(parents=True, exist_ok=True)
                json_disk_path.rename(new_json_disk_path)
                logger.info(f"Renamed JSON: {old_name} -> {new_name}")
            except OSError as e:
                logger.error(f"Failed to rename {json_disk_path}: {e}. Skipping row.")
                continue
        else:
            logger.warning(
                f"JSON file {json_disk_path} not found on disk; updating DB only."
            )

        if csv_row is not None:
            old_csv_disk_path = datastore_path / csv_row.path
            new_csv_disk_path = datastore_path / new_csv_path

            if old_csv_disk_path.exists():
                try:
                    new_csv_disk_path.parent.mkdir(parents=True, exist_ok=True)
                    old_csv_disk_path.rename(new_csv_disk_path)
                    logger.info(f"Renamed CSV: {old_csv_name} -> {new_csv_name}")
                except OSError as e:
                    logger.warning(f"Failed to rename {old_csv_disk_path}: {e}.")
            else:
                logger.warning(
                    f"CSV file {old_csv_disk_path} not found on disk; updating DB only."
                )

        # --- rewrite JSON metadata.data_filename ---
        if new_json_disk_path.exists():
            try:
                with open(new_json_disk_path) as f:
                    data = json.load(f)

                if (
                    isinstance(data, dict)
                    and isinstance(data.get("metadata"), dict)
                    and "data_filename" in data["metadata"]
                ):
                    data["metadata"]["data_filename"] = new_csv_name
                    with open(new_json_disk_path, "w") as f:
                        json.dump(data, f, indent=2)
                    logger.info(f"Updated metadata.data_filename in {new_name}.")
                else:
                    logger.debug(
                        f"No metadata.data_filename key found in {new_name}; skipping rewrite."
                    )
            except Exception as e:
                logger.warning(
                    f"Could not rewrite JSON metadata for {new_name}: {e}. Skipping rewrite."
                )

        # --- update DB: JSON row ---
        connection.execute(
            sa.text(
                "UPDATE files "
                "SET name = :name, path = :path, version_major = 1, "
                "last_modified_date = CURRENT_TIMESTAMP, software_version = :software_version "
                "WHERE id = :id"
            ),
            {
                "name": new_name,
                "path": new_path,
                "id": file_id,
                "software_version": __version__,
            },
        )
        logger.info(f"Updated DB record {file_id}: {old_name} -> {new_name}.")

        # --- update DB: CSV row ---
        if csv_row is not None:
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
    """Run the layer file rename migration."""
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
            "Skipping layer file rename migration."
        )
        return

    _run_migration(op.get_bind(), datastore)


def downgrade() -> None:
    pass  # Intentional no-op: reverting on-disk renames is not supported.
