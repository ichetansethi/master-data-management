"""change leads org_id from uuid to 10-digit bigint

Revision ID: b2c3d4e5f6a7
Revises: 1f5ceb427eb3
Create Date: 2026-09-17 12:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from app.org_id import ORG_ID_MAX, ORG_ID_MIN

revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, Sequence[str], None] = "1f5ceb427eb3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_UUID_TO_TEN_DIGIT = (
    f"({ORG_ID_MIN} + (('x' || substr(replace({{col}}::text, '-', ''), 1, 8))"
    f"::bit(32)::bigint % {ORG_ID_MAX - ORG_ID_MIN + 1}))"
)


def upgrade() -> None:
    op.execute(
        sa.text(
            "ALTER TABLE leads ALTER COLUMN org_id TYPE BIGINT USING "
            + _UUID_TO_TEN_DIGIT.format(col="org_id")
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE lead_ingestion_batches ALTER COLUMN org_id TYPE BIGINT USING "
            + _UUID_TO_TEN_DIGIT.format(col="org_id")
        )
    )
    op.create_check_constraint(
        "ck_leads_org_id_10_digits",
        "leads",
        f"org_id BETWEEN {ORG_ID_MIN} AND {ORG_ID_MAX}",
    )
    op.create_check_constraint(
        "ck_lead_ingestion_batches_org_id_10_digits",
        "lead_ingestion_batches",
        f"org_id BETWEEN {ORG_ID_MIN} AND {ORG_ID_MAX}",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_lead_ingestion_batches_org_id_10_digits",
        "lead_ingestion_batches",
        type_="check",
    )
    op.drop_constraint("ck_leads_org_id_10_digits", "leads", type_="check")
    op.execute(sa.text("ALTER TABLE lead_ingestion_batches ALTER COLUMN org_id TYPE UUID USING NULL"))
    op.execute(sa.text("ALTER TABLE leads ALTER COLUMN org_id TYPE UUID USING NULL"))
