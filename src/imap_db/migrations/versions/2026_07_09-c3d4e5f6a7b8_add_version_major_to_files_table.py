"""Add version_major to files table

Revision ID: c3d4e5f6a7b8
Revises: 52c7b098641d
Create Date: 2026-07-09 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c3d4e5f6a7b8"
down_revision = "52c7b098641d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("files", sa.Column("version_major", sa.Integer(), server_default="0"))
    op.execute(
        sa.text(
            "UPDATE files SET version_major = 1 WHERE path LIKE '%/science/%' AND deletion_date IS NULL"
        )
    )


def downgrade() -> None:
    op.drop_column("files", "version_major")
