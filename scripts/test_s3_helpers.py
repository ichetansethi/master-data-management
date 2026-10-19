import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select

from app.database_app import AsyncSessionLocal_app
from app.models.clients import Clients  # noqa: F401 — needed for org_id FK
from app.models.connector import Connector, ConnectorType
from app.services.s3_client import (
    delete_object,
    folder_key,
    get_object_bytes,
    list_keys,
    put_object_bytes,
)

PAYLOAD = b"customer_id,phone_number\nh1,9999999999\n"


async def load_test_connector():
    async with AsyncSessionLocal_app() as session:
        result = await session.execute(
            select(Connector).where(
                Connector.type == ConnectorType.S3,
                Connector.api_key == "test_s3_api_key",
                Connector.is_active.is_(True),
            )
        )
        return result.scalar_one_or_none()


async def main():
    connector = await load_test_connector()
    if connector is None:
        print("No active S3 test connector found (api_key=test_s3_api_key).")
        print("Run: python scripts/seed_test_connector.py")
        return

    ingestion_key = folder_key(connector, "ingestion", "helper_test.csv")
    success_key = folder_key(connector, "success", "helper_test.csv")
    prior_ingestion = await list_keys(connector, "ingestion")
    prior_success = await list_keys(connector, "success")
    print(f"connector_id={connector.id}")
    print(f"ingestion_key={ingestion_key}")
    print(f"success_key={success_key}")
    print(f"ingestion before: {prior_ingestion}")

    try:
        await put_object_bytes(connector, ingestion_key, PAYLOAD)

        ingestion_keys = await list_keys(connector, "ingestion")
        print(f"ingestion after put: {ingestion_keys}")
        assert ingestion_key in ingestion_keys, ingestion_keys

        downloaded = await get_object_bytes(connector, ingestion_key)
        assert downloaded == PAYLOAD

        await put_object_bytes(connector, success_key, PAYLOAD)
        await delete_object(connector, ingestion_key)

        ingestion_after = await list_keys(connector, "ingestion")
        success_keys = await list_keys(connector, "success")
        print(f"ingestion after delete: {ingestion_after}")
        print(f"success after copy: {success_keys}")
        assert set(ingestion_after) == set(prior_ingestion) - {ingestion_key}, ingestion_after
        assert set(success_keys) - set(prior_success) == {success_key}, success_keys
        print("ok")
    finally:
        await delete_object(connector, success_key)
        await delete_object(connector, ingestion_key)
        ingestion_left = await list_keys(connector, "ingestion")
        success_left = await list_keys(connector, "success")
        print(f"ingestion after cleanup: {ingestion_left}")
        print(f"success after cleanup: {success_left}")


if __name__ == "__main__":
    asyncio.run(main())
