"""Rename download_progress to work_progress

Revision ID: d9fc61463dd6
Revises: f7dab3ec3e42
Create Date: 2025-08-26 16:34:16.648673

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "d9fc61463dd6"
down_revision = "f7dab3ec3e42"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.rename_table("download_progress", "workflow_progress")
    op.execute("ALTER INDEX download_progress_pkey RENAME TO workflow_progress_pkey")


def downgrade() -> None:
    op.rename_table("workflow_progress", "download_progress")
    op.execute("ALTER INDEX workflow_progress_pkey RENAME TO download_progress_pkey")
