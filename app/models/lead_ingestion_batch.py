import uuid
import enum
from datetime import datetime

from sqlalchemy import BigInteger, CheckConstraint, Enum, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB, TIMESTAMP

from app.database import Base
from app.models.leads import LeadSource
from app.org_id import ORG_ID_MAX, ORG_ID_MIN

class LeadIngestionBatchStatus(str, enum.Enum):
    PROCESSING = "processing"
    COMPLETED_SUCCESSFULLY = "completed_successfully"
    COMPLETED_WITH_ERRORS = "completed_with_errors"
    FAILED = "failed"
    PENDING = "pending"


class LeadIngestionBatch(Base):

    __tablename__ = "lead_ingestion_batches"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    connector_id: Mapped[uuid.UUID|None] = mapped_column(PG_UUID, nullable=True)
    status: Mapped[LeadIngestionBatchStatus] = mapped_column(Enum(LeadIngestionBatchStatus), nullable=False, default=LeadIngestionBatchStatus.PENDING)
    source: Mapped[LeadSource] = mapped_column(Enum(LeadSource), nullable=False)
    source_ref: Mapped[str] = mapped_column(String, nullable=True)
    total_leads: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    success_leads: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_leads: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_messages: Mapped[list[dict]] = mapped_column(JSONB, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=func.now())
    completed_at: Mapped[datetime|None] = mapped_column(TIMESTAMP, nullable=True)

    __table_args__ = (
        CheckConstraint(
            f"org_id BETWEEN {ORG_ID_MIN} AND {ORG_ID_MAX}",
            name="ck_lead_ingestion_batches_org_id_10_digits",
        ),
    )
