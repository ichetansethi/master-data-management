import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select

from app.database_app import AsyncSessionLocal_app
from app.models.clients import Clients
from app.models.roles import Roles
from app.models.users import Users
from app.security import hash_password


async def seed_admin_user():
    async with AsyncSessionLocal_app() as session:
        admin_role = await session.execute(select(Roles).where(Roles.name == "ADMIN"))
        admin_role = admin_role.scalar_one_or_none()
        if admin_role is None:
            raise ValueError("ADMIN role not found")
        existing_user = await session.execute(select(Users).where(Users.username == "admin"))
        existing_user = existing_user.scalar_one_or_none()
        if existing_user is None:
            user = Users(
                username="admin",
                name="Admin User",
                mobile_number="1234567890",
                email="admin@example.com",
                hashed_password=hash_password("some-test-password"),
                role_id=admin_role.id,
                is_active=True,
                org_id=None,
            )
            session.add(user)
        await session.commit()


if __name__ == "__main__":
    asyncio.run(seed_admin_user())
