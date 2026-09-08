# scripts/seed_test_agent.py
import asyncio
from sqlalchemy import select
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.database_app import AsyncSessionLocal_app
from app.models.clients import Clients  # noqa: F401 — needed for users.org_id FK
from app.models.roles import Roles
from app.models.users import Users
from app.security import hash_password


async def seed_test_agent():
    async with AsyncSessionLocal_app() as session:
        agent_role = await session.execute(select(Roles).where(Roles.name == "AGENT"))
        agent_role = agent_role.scalar_one_or_none()
        if agent_role is None:
            raise ValueError("AGENT role not found")

        existing_user = await session.execute(select(Users).where(Users.username == "testagent"))
        existing_user = existing_user.scalar_one_or_none()
        if existing_user is None:
            user = Users(
                username="testagent",
                name="Test Agent",
                mobile_number="1234567890",
                email="testagent@example.com",
                hashed_password=hash_password("agent-test-password"),
                role_id=agent_role.id,
                is_active=True,
                org_id=None,
            )
            session.add(user)
        await session.commit()


if __name__ == "__main__":
    asyncio.run(seed_test_agent())