"""Add descriptor field as nullable

Revision ID: a1b2c3d4e5f6
Revises: 44799fd8de27
Create Date: 2026-01-19 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

from imap_db.model import File

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "44799fd8de27"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add the descriptor column as nullable first
    op.add_column("files", sa.Column("descriptor", sa.String(128), nullable=True))

    # Populate the descriptor field from existing file names
    connection = op.get_bind()
    files = connection.execute(sa.text("SELECT id, name FROM files"))

    for file_id, name in files:
        descriptor = File.get_descriptor_from_filename(name)
        connection.execute(
            sa.text("UPDATE files SET descriptor = :descriptor WHERE id = :id"),
            {"descriptor": descriptor, "id": file_id},
        )


def downgrade() -> None:
    op.drop_column("files", "descriptor")
