import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.database_app import AsyncSessionLocal_app
from app.models.roles import Roles
from sqlalchemy import select

ROLE_NAMES = ["ADMIN", "ORG_ADMIN", "SUPERVISOR", "AGENT", "AUDITOR", "CAMPAIGN_MANAGER"]


async def seed_roles():
    async with AsyncSessionLocal_app() as session:
        for name in ROLE_NAMES:
            result = await session.execute(select(Roles).where(Roles.name == name))
            existing = result.scalar_one_or_none()
            if existing is None:
                role = Roles(name=name)
                session.add(role)
        await session.commit()


if __name__ == "__main__":
    asyncio.run(seed_roles())