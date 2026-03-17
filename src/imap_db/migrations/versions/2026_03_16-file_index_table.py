"""Add file_index table

Revision ID: a1b2c3d4e5f7
Revises: 4fdab0d788f0
Create Date: 2026-03-16 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "a1b2c3d4e5f7"
down_revision = "4fdab0d788f0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "file_index",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("file_id", sa.Integer(), nullable=False),
        sa.Column(
            "indexed_date",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("record_count", sa.Integer(), nullable=True),
        sa.Column("first_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("has_gaps", sa.Boolean(), nullable=True),
        sa.Column("has_missing_data", sa.Boolean(), nullable=True),
        sa.Column("has_bad_data", sa.Boolean(), nullable=True),
        sa.Column("total_time_without_gaps", sa.Interval(), nullable=True),
        sa.Column("total_gap_duration", sa.Interval(), nullable=True),
        sa.Column("gaps", sa.JSON(), nullable=True),
        sa.Column("nan_gaps", sa.JSON(), nullable=True),
        sa.Column("missing_data_gaps", sa.JSON(), nullable=True),
        sa.Column("cdf_attributes", sa.JSON(), nullable=True),
        sa.Column("column_stats", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["file_id"], ["files.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("file_id"),
    )


def downgrade() -> None:
    op.drop_table("file_index")
