"""Fix layer JSON metadata.data_filename missing the "-data" descriptor, then rename CSV.

Migration ``e5f6a7b8c9d0`` (rename_legacy_layer_csv_files) tried to complete the
CSV rename left unfinished by ``d4e5f6a7b8c9``, by reading each layer JSON's
``metadata.data_filename`` and deriving the legacy CSV name from it. That relied
on ``data_filename`` already being correct -- but for many layer files it is
itself wrong: it was written as the JSON's own base name with the extension
swapped (e.g. ``imap_mag_manual-burst-layer_20260501_v001.0001.csv``), omitting
the ``-data`` descriptor that the real CSV file actually uses (e.g.
``imap_mag_manual-burst-layer-data_20260501_v001.0001.csv``). Because of that,
Migration ``e5f6a7b8c9d0`` derived a legacy name that never matched a real
database record, so it renamed nothing for these files.

This migration, for each active layer JSON file:
  1. Reads ``metadata.data_filename``.
  2. If it is missing the ``-data`` descriptor (i.e. does not contain
     ``-layer-data_``), corrects it by inserting ``-data`` after ``-layer``,
     and rewrites the JSON file with the corrected value.
  3. Derives the legacy CSV name by converting the new-style version suffix
     (``_v001.NNNN``) back to the legacy suffix (``_vNNN``).
  4. Looks up the CSV's database record under its legacy name.
  5. Renames the CSV file on disk to the corrected new-style name.
  6. Updates the CSV's ``files`` row (name, path, version_major) to match.

The downgrade is intentionally a no-op: reverting filename/content changes on
disk is error-prone and the old naming scheme is considered superseded.

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-07-18 00:00:00.000000

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
revision = "f6a7b8c9d0e1"
down_revision = "e5f6a7b8c9d0"
branch_labels = None
depends_on = None

logger = logging.getLogger(__name__)

_LAYER_JSON_LIKE = "%-layer%.json"
_NEW_CSV_VERSION_RE = re.compile(r"_v001\.(\d+)\.csv$")
_MISSING_DATA_DESCRIPTOR_RE = re.compile(r"-layer_")


def _fix_missing_data_descriptor(csv_name: str) -> str | None:
    """Insert the missing ``-data`` descriptor into a layer CSV filename.

    Args:
        csv_name: The (possibly incorrect) CSV filename read from
            metadata.data_filename.

    Returns:
        The corrected CSV filename, or None if csv_name already contains the
        ``-data`` descriptor (nothing to fix) or does not match the expected
        ``-layer_`` pattern at all.
    """
    if "-layer-data_" in csv_name:
        return None

    match = _MISSING_DATA_DESCRIPTOR_RE.search(csv_name)
    if not match:
        return None

    return csv_name[: match.start()] + "-layer-data_" + csv_name[match.end() :]


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
    """Fix data_filename typos and rename legacy layer CSV files.

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

        raw_csv_name = (
            data.get("metadata", {}).get("data_filename")
            if isinstance(data, dict)
            else None
        )
        if not raw_csv_name:
            logger.debug(f"No metadata.data_filename found in {json_name}; skipping.")
            continue

        new_csv_name = _fix_missing_data_descriptor(raw_csv_name)
        if new_csv_name is None:
            logger.debug(
                f"metadata.data_filename {raw_csv_name!r} in {json_name} already "
                "correct or unrecognised. Skipping."
            )
            continue

        # --- already renamed? ---
        already_done = connection.execute(
            sa.text(
                "SELECT id FROM files WHERE name = :name AND deletion_date IS NULL"
            ),
            {"name": new_csv_name},
        ).fetchone()
        if already_done is None:
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
            else:
                new_csv_path = csv_row.path.replace(old_csv_name, new_csv_name)

                # --- rename CSV file on disk ---
                old_csv_disk_path = datastore_path / csv_row.path
                new_csv_disk_path = datastore_path / new_csv_path

                renamed = True
                if old_csv_disk_path.exists():
                    try:
                        new_csv_disk_path.parent.mkdir(parents=True, exist_ok=True)
                        old_csv_disk_path.rename(new_csv_disk_path)
                        logger.info(f"Renamed CSV: {old_csv_name} -> {new_csv_name}")
                    except OSError as e:
                        logger.error(
                            f"Failed to rename {old_csv_disk_path}: {e}. "
                            "Skipping DB update for this row."
                        )
                        renamed = False
                else:
                    logger.warning(
                        f"CSV file {old_csv_disk_path} not found on disk; "
                        "updating DB only."
                    )

                if renamed:
                    # --- update DB: CSV row ---
                    connection.execute(
                        sa.text(
                            "UPDATE files "
                            "SET name = :name, path = :path, version_major = 1, "
                            "last_modified_date = CURRENT_TIMESTAMP, "
                            "software_version = :software_version "
                            "WHERE id = :id"
                        ),
                        {
                            "name": new_csv_name,
                            "path": new_csv_path,
                            "id": csv_row.id,
                            "software_version": __version__,
                        },
                    )
                    logger.info(
                        f"Updated DB record for CSV {old_csv_name} -> {new_csv_name}."
                    )

        # --- rewrite JSON metadata.data_filename ---
        if raw_csv_name != new_csv_name:
            data["metadata"]["data_filename"] = new_csv_name
            with open(json_disk_path, "w") as f:
                json.dump(data, f, indent=2)
            logger.info(
                f"Fixed metadata.data_filename in {json_name}: "
                f"{raw_csv_name} -> {new_csv_name}."
            )


def upgrade() -> None:
    """Run the layer CSV data_filename fix and rename migration."""
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
            "Skipping layer CSV data_filename fix migration."
        )
        return

    _run_migration(op.get_bind(), datastore)


def downgrade() -> None:
    pass  # Intentional no-op: reverting on-disk renames is not supported.
