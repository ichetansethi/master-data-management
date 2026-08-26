import uuid
import enum
from datetime import datetime

from sqlalchemy import String, UniqueConstraint, Enum, Boolean, ForeignKey, ForeignKeyConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB, TIMESTAMP

from app.database_app import BaseApp

class ConnectorType(str, enum.Enum):
    API = "api"
    CSV = "csv"
    S3 = "s3"

class Connector(BaseApp):

    __tablename__ = "connectors"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID, primary_key=True, default=uuid.uuid4)
    org_id: Mapped[uuid.UUID] = mapped_column(PG_UUID, ForeignKey("clients.id"), nullable=False)
    type: Mapped[ConnectorType] = mapped_column(Enum(ConnectorType), nullable=False)
    api_key: Mapped[str] = mapped_column(String, nullable=False)
    api_secret: Mapped[str] = mapped_column(String, nullable=False)
    field_mapping: Mapped[list[dict]] = mapped_column(JSONB, nullable=False)
    notification_email: Mapped[str] = mapped_column(String, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=func.now(), onupdate=func.now())
