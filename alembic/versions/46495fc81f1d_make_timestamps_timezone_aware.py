"""make timestamp columns timezone-aware (TIMESTAMPTZ)

Revision ID: 46495fc81f1d
Revises: b2c3d4e5f6a7
Create Date: 2026-09-22 13:37:23.973898

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "46495fc81f1d"
down_revision: Union[str, Sequence[str], None] = "b2c3d4e5f6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# (table, column) pairs whose existing naive values were always written as
# UTC wall-clock time: created_at/updated_at come from Postgres func.now()
# under a UTC-timezone session, and completed_at came from the app's
# datetime.now() (now fixed to datetime.now(timezone.utc) in
# app/api/leads_upload.py). Relabeling them as UTC via `AT TIME ZONE 'UTC'`
# is a reinterpretation, not a value conversion.
_COLUMNS = (
    ("leads", "created_at"),
    ("leads", "updated_at"),
    ("lead_ingestion_batches", "created_at"),
    ("lead_ingestion_batches", "completed_at"),
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
