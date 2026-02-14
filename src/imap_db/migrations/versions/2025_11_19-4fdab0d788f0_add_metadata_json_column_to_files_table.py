"""Add metadata JSON column to files table

Revision ID: 4fdab0d788f0
Revises: 44799fd8de27
Create Date: 2025-11-19 18:35:16.985261

"""

import sqlalchemy as sa
from alembic import op

revision = "4fdab0d788f0"
down_revision = "44799fd8de27"
branch_labels = None
depends_on = None
constraint = "UQ_workflow_progress_item_name"


def upgrade() -> None:
    # New column
    op.add_column("files", sa.Column("file_meta", sa.JSON(), nullable=True))
    ## this was missing from earlier migrations, not sure why
    op.create_unique_constraint(constraint, "workflow_progress", ["item_name"])


def downgrade() -> None:
    op.drop_constraint(
        constraint_name=constraint, table_name="workflow_progress", type_="unique"
    )
    op.drop_column("files", "file_meta")
