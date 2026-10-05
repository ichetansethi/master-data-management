import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select

from app.database_app import AsyncSessionLocal_app
from app.models.clients import Clients
from app.models.connector import Connector, ConnectorType
import aioboto3

from app.services.ingestion import R2_ENDPOINT_URL, list_pending_files


async def main():
    async with AsyncSessionLocal_app() as session:
        result = await session.execute(
            select(Connector).where(
                Connector.type == ConnectorType.S3,
                Connector.api_key == "test_s3_api_key",
                Connector.is_active.is_(True),
            )
        )
        connector = result.scalar_one_or_none()

    if connector is None:
        print("No active S3 test connector found (api_key=test_s3_api_key).")
        print("Run: python scripts/seed_test_connector.py")
        return

    config = connector.connection_config or {}
    prefix = config.get("prefix", "")
    print(f"connector_id={connector.id}")
    print(f"bucket={config.get('bucket')}")
    print(f"listing prefix={prefix}ingestion/")

    keys = await list_pending_files(connector)
    print(f"count={len(keys)}")
    for key in keys:
        print(key)

    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
    ) as s3:
        response = await s3.list_objects_v2(Bucket=config["bucket"])
        print("all keys:", [obj["Key"] for obj in response.get("Contents", [])])

if __name__ == "__main__":
    asyncio.run(main())
