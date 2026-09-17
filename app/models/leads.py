import uuid
import enum
from datetime import datetime

from sqlalchemy import BigInteger, CheckConstraint, Enum, Index, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB, TIMESTAMP

from app.database import Base
from app.org_id import ORG_ID_MAX, ORG_ID_MIN

class LeadSource(str, enum.Enum):
    API = "api"
    CSV = "csv"
    S3 = "s3"


class LeadStatus(str, enum.Enum):
    PENDING = "pending"
    VALIDATED = "validated"
    REJECTED = "rejected"


class Lead(Base):

    __tablename__ = "leads"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    customer_id: Mapped[str] = mapped_column(String, nullable=False)
    phone_number: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[LeadSource] = mapped_column(Enum(LeadSource), nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(String, nullable=True)
    dedup_hash: Mapped[str] = mapped_column(String, nullable=False)
    lead_attributes: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    status: Mapped[LeadStatus] = mapped_column(Enum(LeadStatus), nullable=False, default=LeadStatus.PENDING)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("org_id", "idempotency_key", name="uix_org_id_idempotency_key"),
        UniqueConstraint("org_id", "customer_id", name="uix_org_id_customer_id"),
        Index("idx_org_id_dedup_hash", "org_id", "dedup_hash"),
        CheckConstraint(
            f"org_id BETWEEN {ORG_ID_MIN} AND {ORG_ID_MAX}",
            name="ck_leads_org_id_10_digits",
        ),
    )