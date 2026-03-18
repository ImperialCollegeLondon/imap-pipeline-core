"""Add timestamp delta columns to file_index

Revision ID: b2c3d4e5f6a8
Revises: a1b2c3d4e5f7
Create Date: 2026-03-18 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "b2c3d4e5f6a8"
down_revision = "a1b2c3d4e5f7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "file_index",
        sa.Column("min_delta_between_timestamps", sa.Interval(), nullable=True),
    )
    op.add_column(
        "file_index",
        sa.Column("max_delta_between_timestamps", sa.Interval(), nullable=True),
    )
    op.add_column(
        "file_index",
        sa.Column("avg_delta_between_timestamps", sa.Interval(), nullable=True),
    )
    op.add_column(
        "file_index",
        sa.Column("median_delta_between_timestamps", sa.Interval(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("file_index", "min_delta_between_timestamps")
    op.drop_column("file_index", "max_delta_between_timestamps")
    op.drop_column("file_index", "avg_delta_between_timestamps")
    op.drop_column("file_index", "median_delta_between_timestamps")
