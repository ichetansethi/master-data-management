import uuid
import enum
from datetime import datetime

from sqlalchemy import String, Enum, UniqueConstraint, Index, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB, TIMESTAMP

from app.database_app import BaseApp

class ClientStatus(str, enum.Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"

class Client(BaseApp):

    __tablename__ = "clients"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    status: Mapped[ClientStatus] = mapped_column(Enum(ClientStatus), nullable=False, default=ClientStatus.ACTIVE)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

__table_args__ = (
    UniqueConstraint("id", "name", name="uix_id_name"),
)