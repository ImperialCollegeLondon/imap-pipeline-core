"""Make descriptor non-nullable and add unique constraint

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-01-19 12:01:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b2c3d4e5f6a7"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Make descriptor non-nullable
    op.alter_column("files", "descriptor", existing_type=sa.String(128), nullable=False)

    # Add unique constraint on descriptor, content_date, and version
    op.create_unique_constraint(
        "uq_files_descriptor_content_date_version_deletion_date",
        "files",
        ["descriptor", "content_date", "version", "deletion_date"],
    )


def downgrade() -> None:
    # Drop the unique constraint
    op.drop_constraint(
        "uq_files_descriptor_content_date_version", "files", type_="unique"
    )

    # Make descriptor nullable again
    op.alter_column("files", "descriptor", existing_type=sa.String(128), nullable=True)
