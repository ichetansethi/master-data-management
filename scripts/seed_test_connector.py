import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.database_app import AsyncSessionLocal_app
from app.models.connector import Connector, ConnectorType
from sqlalchemy import select
from app.models.clients import Clients


async def seed_test_connector():
    async with AsyncSessionLocal_app() as session:
        existing = await session.execute(
            select(Connector).where(
                Connector.org_id == 1000000000,
                Connector.api_key == "test_api_key",
            )
        )
        existing = existing.scalar_one_or_none()
        if existing is None:
            connector = Connector(type=ConnectorType.API, api_key="test_api_key", api_secret="test_api_secret", is_active=True, org_id=1000000000, field_mapping=[], notification_email="test@example.com")
            session.add(connector)
            await session.commit()
            print(f"Created connector with api_key: {connector.api_key}")
        else:
            print(f"Connector already exists with api_key: {existing.api_key}")


if __name__ == "__main__":
    asyncio.run(seed_test_connector())