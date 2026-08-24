"""expand leads table to full schema

Revision ID: 6c42aa1ac577
Revises: ee3852be06ec
Create Date: 2026-08-17 12:27:49.044759

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from sqlalchemy.dialects.postgresql import ENUM

# revision identifiers, used by Alembic.
revision: str = '6c42aa1ac577'
down_revision: Union[str, Sequence[str], None] = 'ee3852be06ec'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


leadsource_enum = ENUM('API', 'CSV', 'S3', name='leadsource')
leadstatus_enum = ENUM('PENDING', 'VALIDATED', 'REJECTED', name='leadstatus')


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    leadsource_enum.create(bind, checkfirst=True)
    leadstatus_enum.create(bind, checkfirst=True)

    op.add_column('leads', sa.Column('phone_number', sa.String(), nullable=False))
    op.add_column('leads', sa.Column('source', leadsource_enum, nullable=False))
    op.add_column('leads', sa.Column('idempotency_key', sa.String(), nullable=True))
    op.add_column('leads', sa.Column('dedup_hash', sa.String(), nullable=False))
    op.add_column('leads', sa.Column('lead_attributes', postgresql.JSONB(astext_type=sa.Text()), nullable=False))
    op.add_column('leads', sa.Column('status', leadstatus_enum, nullable=False))
    op.add_column('leads', sa.Column('created_at', postgresql.TIMESTAMP(), server_default=sa.text('now()'), nullable=False))
    op.add_column('leads', sa.Column('updated_at', postgresql.TIMESTAMP(), server_default=sa.text('now()'), nullable=False))
    op.create_index('idx_org_id_dedup_hash', 'leads', ['org_id', 'dedup_hash'], unique=False)
    op.create_unique_constraint('uix_org_id_customer_id', 'leads', ['org_id', 'customer_id'])
    op.create_unique_constraint('uix_org_id_idempotency_key', 'leads', ['org_id', 'idempotency_key'])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('uix_org_id_idempotency_key', 'leads', type_='unique')
    op.drop_constraint('uix_org_id_customer_id', 'leads', type_='unique')
    op.drop_index('idx_org_id_dedup_hash', table_name='leads')
    op.drop_column('leads', 'updated_at')
    op.drop_column('leads', 'created_at')
    op.drop_column('leads', 'status')
    op.drop_column('leads', 'lead_attributes')
    op.drop_column('leads', 'dedup_hash')
    op.drop_column('leads', 'idempotency_key')
    op.drop_column('leads', 'source')
    op.drop_column('leads', 'phone_number')

    bind = op.get_bind()
    leadsource_enum.drop(bind, checkfirst=True)
    leadstatus_enum.drop(bind, checkfirst=True)