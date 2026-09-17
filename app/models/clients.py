import enum
from datetime import datetime

from sqlalchemy import BigInteger, CheckConstraint, String, Enum, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy import Identity

from app.database_app import BaseApp
from app.org_id import ORG_ID_MAX, ORG_ID_MIN

class ClientStatus(str, enum.Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"

class Clients(BaseApp):

    __tablename__ = "clients"

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(start=ORG_ID_MIN, increment=1),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    status: Mapped[ClientStatus] = mapped_column(Enum(ClientStatus), nullable=False, default=ClientStatus.ACTIVE)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        CheckConstraint(
            f"id BETWEEN {ORG_ID_MIN} AND {ORG_ID_MAX}",
            name="ck_clients_id_10_digits",
        ),
    )