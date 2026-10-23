"""Add timestamp delta columns to file_analysis

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
TABLE_NAME = "file_analysis"


def upgrade() -> None:
    op.add_column(
        TABLE_NAME,
        sa.Column("min_delta_between_timestamps", sa.Interval(), nullable=True),
    )
    op.add_column(
        TABLE_NAME,
        sa.Column("max_delta_between_timestamps", sa.Interval(), nullable=True),
    )
    op.add_column(
        TABLE_NAME,
        sa.Column("avg_delta_between_timestamps", sa.Interval(), nullable=True),
    )
    op.add_column(
        TABLE_NAME,
        sa.Column("median_delta_between_timestamps", sa.Interval(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column(TABLE_NAME, "min_delta_between_timestamps")
    op.drop_column(TABLE_NAME, "max_delta_between_timestamps")
    op.drop_column(TABLE_NAME, "avg_delta_between_timestamps")
    op.drop_column(TABLE_NAME, "median_delta_between_timestamps")
