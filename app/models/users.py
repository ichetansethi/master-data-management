import uuid
from datetime import datetime

from sqlalchemy import BigInteger, Boolean, CheckConstraint, ForeignKeyConstraint, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, TIMESTAMP

from app.database_app import BaseApp
from app.org_id import ORG_ID_MAX, ORG_ID_MIN


class Users(BaseApp):

    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID, primary_key=True, default=uuid.uuid4)
    username: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    mobile_number: Mapped[str] = mapped_column(String, nullable=False)
    email: Mapped[str] = mapped_column(String, nullable=False)
    hashed_password: Mapped[str] = mapped_column(String, nullable=False)
    org_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    role_id: Mapped[uuid.UUID] = mapped_column(PG_UUID, nullable=False)
    reporting_manager_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        ForeignKeyConstraint(["org_id"], ["clients.id"]),
        ForeignKeyConstraint(["role_id"], ["roles.id"]),
        ForeignKeyConstraint(["reporting_manager_id"], ["users.id"]),
        UniqueConstraint("org_id", "username", name="uix_org_id_username"),
        UniqueConstraint("email", name="uix_email"),
        CheckConstraint(
            f"org_id IS NULL OR org_id BETWEEN {ORG_ID_MIN} AND {ORG_ID_MAX}",
            name="ck_users_org_id_10_digits",
        ),
    )