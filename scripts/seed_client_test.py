import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select

from app.database_app import AsyncSessionLocal_app
from app.models.clients import Clients


async def seed_test_client():
    async with AsyncSessionLocal_app() as session:
        existing = await session.execute(select(Clients).where(Clients.name == "Test Bank"))
        existing = existing.scalar_one_or_none()
        if existing is None:
            client = Clients(name="Test Bank")
            session.add(client)
            await session.commit()
            await session.refresh(client)
            print(f"Created client with id: {client.id}")
        else:
            print(f"Client already exists with id: {existing.id}")


if __name__ == "__main__":
    asyncio.run(seed_test_client())