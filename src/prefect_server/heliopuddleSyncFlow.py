"""Prefect flow that keeps heliopuddle in sync with the IMAP files table."""

import logging
from datetime import datetime

from prefect import flow
from prefect.states import Completed, Failed

from imap_mag.config.AppSettings import AppSettings
from imap_mag.db import Database
from imap_mag.db.HeliopuddleSync import HeliopuddleSyncError, run_sync
from prefect_server.constants import PREFECT_CONSTANTS

logger = logging.getLogger(__name__)


@flow(
    name=PREFECT_CONSTANTS.FLOW_NAMES.HELIOPUDDLE_SYNC,
    log_prints=True,
)
def heliopuddle_sync_flow(
    full: bool = False,
    since: datetime | None = None,
    remove_untracked: bool = False,
    dry_run: bool = False,
):
    """Bring heliopuddle in line with the IMAP files table.

    The first run (or a run with full=True) compares every file, which is the
    one-off migration. Later runs only re-sync files modified since the last run,
    repairing any change that could not be applied when it was committed.

    Args:
        full: Compare every file instead of only recently modified ones.
        since: Sync files modified after this time instead of since the last run.
        remove_untracked: With full, remove files that are only in heliopuddle.
        dry_run: Report what would change without changing heliopuddle.
    """
    app_settings = AppSettings()  # type: ignore
    if not app_settings.heliopuddle.enabled:
        return Completed(
            message="heliopuddle integration is disabled",
            name=PREFECT_CONSTANTS.SKIPPED_STATE_NAME,
        )

    try:
        summary = run_sync(
            app_settings,
            Database(),
            full=full,
            since=since,
            remove_untracked=remove_untracked,
            dry_run=dry_run,
        )
    except HeliopuddleSyncError as e:
        return Failed(message=str(e))

    return Completed(message=f"heliopuddle sync: {summary.describe()}")
