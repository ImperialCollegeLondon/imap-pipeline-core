import logging
import os
from pathlib import Path

import sqlalchemy as sa
from alembic import op

from imap_mag.util import DatetimeProvider

revision = "d910e3b4bc3d"
down_revision = "f6a7b8c9d0e1"
branch_labels = None
depends_on = None

logger = logging.getLogger(__name__)


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


def _run_migration(
    connection: sa.engine.Connection,
    datastore: Path,
    datetime_provider: DatetimeProvider = DatetimeProvider(),
) -> None:

    from imap_mag.io.file import IALiRTPathHandler

    rows = connection.execute(
        sa.text(
            "SELECT id, name, path FROM files "
            "WHERE name ~ '^imap_ialirt_[0-9]{8}\\.csv$' "
            "AND deletion_date IS NULL"
        )
    ).fetchall()

    logger.info(f"Found {len(rows)} files to rename to include 'mag'.")

    now = datetime_provider.now()

    for file_id, name, path in rows:
        logger.info(f"looping through {file_id}, {name}, {path}")
        handler = IALiRTPathHandler.from_filename(name)

        if not handler:
            logger.warning(
                f"Could not parse filename {name} with IALiRTPathHandler. Skipping."
            )
            continue

        is_legacy_name = IALiRTPathHandler.is_legacy_name(name)

        # new name
        if handler.content_date is not None and is_legacy_name:
            date_str = handler.content_date.strftime("%Y%m%d")
            new_name = f"imap_ialirt_mag_{date_str}.{handler.extension}"

            clean_db_path = path.lstrip("/")

            old_full_path = datastore / clean_db_path
            new_full_path = old_full_path.parent / new_name

            # do the rename
            if old_full_path.exists():
                new_full_path.parent.mkdir(parents=True, exist_ok=True)
                old_full_path.rename(new_full_path)
                logger.info(f"Renamed: {name} -> {new_name}")

                # update db
                connection.execute(
                    sa.text("UPDATE files SET deletion_date = :now WHERE id = :id"),
                    {"now": now, "id": file_id},
                )
            else:
                logger.warning(f"File not found datastore: {old_full_path}. Skipping.")
        else:
            logger.warning(f"Skipping file {name}: No content date found.")


def downgrade() -> None:
    pass
