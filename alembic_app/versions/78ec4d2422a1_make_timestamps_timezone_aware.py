"""make timestamp columns timezone-aware (TIMESTAMPTZ)

Revision ID: 78ec4d2422a1
Revises: 2a4e35a7502c
Create Date: 2026-09-22 13:37:23.973898

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "78ec4d2422a1"
down_revision: Union[str, Sequence[str], None] = "2a4e35a7502c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# All existing values were written by Postgres func.now() under a
# UTC-timezone session, so relabeling as UTC via `AT TIME ZONE 'UTC'` is a
# reinterpretation, not a value conversion.
_COLUMNS = (
    ("clients", "created_at"),
    ("clients", "updated_at"),
    ("connectors", "created_at"),
    ("connectors", "updated_at"),
    ("users", "created_at"),
    ("users", "updated_at"),
)


def upgrade() -> None:
    for table, column in _COLUMNS:
        op.execute(
            sa.text(
                f"ALTER TABLE {table} ALTER COLUMN {column} TYPE TIMESTAMPTZ "
                f"USING {column} AT TIME ZONE 'UTC'"
            )
        )


def downgrade() -> None:
    for table, column in reversed(_COLUMNS):
        op.execute(
            sa.text(
                f"ALTER TABLE {table} ALTER COLUMN {column} TYPE TIMESTAMP "
                f"USING {column} AT TIME ZONE 'UTC'"
            )
        )
