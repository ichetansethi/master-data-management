import uuid
import enum
from datetime import datetime

from sqlalchemy import BigInteger, Boolean, CheckConstraint, Enum, ForeignKey, String, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB, TIMESTAMP

from app.database_app import BaseApp
from app.org_id import ORG_ID_MAX, ORG_ID_MIN

class ConnectorType(str, enum.Enum):
    API = "api"
    CSV = "csv"
    S3 = "s3"

class Connector(BaseApp):

    __tablename__ = "connectors"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("clients.id"), nullable=False)
    type: Mapped[ConnectorType] = mapped_column(Enum(ConnectorType), nullable=False)
    api_key: Mapped[str] = mapped_column(String, nullable=False)
    api_secret: Mapped[str] = mapped_column(String, nullable=False)
    field_mapping: Mapped[list[dict]] = mapped_column(JSONB, nullable=False)
    notification_email: Mapped[str] = mapped_column(String, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        CheckConstraint(
            f"org_id BETWEEN {ORG_ID_MIN} AND {ORG_ID_MAX}",
            name="ck_connectors_org_id_10_digits",
        ),
    )
