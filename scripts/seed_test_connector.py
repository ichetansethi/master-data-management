import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv()

from app.database_app import AsyncSessionLocal_app
from app.models.connector import Connector, ConnectorType
from sqlalchemy import select
from app.models.clients import Clients  # noqa: F401 — needed for org_id FK

TEST_FIELD_MAPPING = [
    {
        "file_column": "customer_id",
        "lead_attribute": "customer_id",
        "mandatory": True,
    },
    {
        "file_column": "phone_number",
        "lead_attribute": "phone_number",
        "data_type": "PRIMARY_PHONE_NUMBER",
        "mandatory": True,
    },
]


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
            connector = Connector(
                type=ConnectorType.API,
                api_key="test_api_key",
                api_secret="test_api_secret",
                is_active=True,
                org_id=1000000000,
                field_mapping=TEST_FIELD_MAPPING,
                notification_email="test@example.com",
            )
            session.add(connector)
            await session.commit()
            print(f"Created connector id={connector.id} api_key={connector.api_key}")
        else:
            existing.field_mapping = TEST_FIELD_MAPPING
            await session.commit()
            print(f"Updated connector id={existing.id} with field_mapping")


async def seed_test_s3_connector():
    bucket = os.environ.get("TEST_CONNECTOR_S3_BUCKET")
    access_key = os.environ.get("TEST_CONNECTOR_S3_ACCESS_KEY_ID")
    secret_key = os.environ.get("TEST_CONNECTOR_S3_SECRET_ACCESS_KEY")
    prefix = os.environ.get("TEST_CONNECTOR_S3_PREFIX") or None

    if not all([bucket, access_key, secret_key]):
        print(
            "Skipping S3 test connector: set TEST_CONNECTOR_S3_BUCKET, "
            "TEST_CONNECTOR_S3_ACCESS_KEY_ID and TEST_CONNECTOR_S3_SECRET_ACCESS_KEY "
            "in .env first."
        )
        return

    connection_config = {
        "bucket": bucket,
        "access_key": access_key,
        "secret_key": secret_key,
    }
    if prefix:
        connection_config["prefix"] = prefix

    async with AsyncSessionLocal_app() as session:
        existing = await session.execute(
            select(Connector).where(
                Connector.org_id == 1000000000,
                Connector.api_key == "test_s3_api_key",
            )
        )
        existing = existing.scalar_one_or_none()
        if existing is None:
            connector = Connector(
                type=ConnectorType.S3,
                api_key="test_s3_api_key",
                api_secret="test_s3_api_secret",
                is_active=True,
                org_id=1000000000,
                field_mapping=TEST_FIELD_MAPPING,
                connection_config=connection_config,
                notification_email="test@example.com",
            )
            session.add(connector)
            await session.commit()
            print(f"Created S3 connector id={connector.id} api_key={connector.api_key}")
        else:
            existing.field_mapping = TEST_FIELD_MAPPING
            existing.connection_config = connection_config
            await session.commit()
            print(f"Updated S3 connector id={existing.id} with field_mapping + connection_config")


async def main():
    await seed_test_connector()
    await seed_test_s3_connector()


if __name__ == "__main__":
    asyncio.run(main())