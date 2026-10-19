import asyncio
import csv
import io
import logging
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from sqlalchemy import delete, select

from app.database import AsyncSessionLocal
from app.database_app import AsyncSessionLocal_app
from app.models.clients import Clients  # noqa: F401 — needed for org_id FK
from app.models.connector import Connector, ConnectorType
from app.models.lead_ingestion_batch import LeadIngestionBatch
from app.models.leads import Leads
from app.services.s3_client import (
    delete_object,
    folder_key,
    get_object_bytes,
    list_keys,
    put_object_bytes,
)
from app.services.s3_ingestion import poll_once

RUN = uuid.uuid4().hex[:8]
GOOD_A = f"poll-{RUN}-a"
GOOD_B = f"poll-{RUN}-b"
CSV_NAME = f"poll-{RUN}.csv"
TXT_NAME = f"poll-{RUN}.txt"
TXT_BODY = b"leave this file alone\n"

MIXED = (
    "customer_id,phone_number\n"
    f"{GOOD_A},9811200001\n"
    f"{GOOD_B},9811200002\n"
    "bad-phone,bad\n"
    ",9811200004\n"
).encode()


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


async def latest_batch(key: str) -> LeadIngestionBatch | None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(LeadIngestionBatch)
            .where(LeadIngestionBatch.source_ref == key)
            .order_by(LeadIngestionBatch.created_at.desc())
        )
        return result.scalars().first()


def parse_csv(data: bytes):
    reader = csv.DictReader(io.StringIO(data.decode("utf-8")))
    return list(reader.fieldnames), list(reader)


class ListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)


async def main():
    connector = await load_test_connector()
    if connector is None:
        print("No active S3 test connector found (api_key=test_s3_api_key).")
        print("Run: python scripts/seed_test_connector.py")
        return

    csv_key = folder_key(connector, "ingestion", CSV_NAME)
    txt_key = folder_key(connector, "ingestion", TXT_NAME)
    preserved = []
    for key in await list_keys(connector, "ingestion"):
        if key.lower().endswith(".csv"):
            preserved.append((key, await get_object_bytes(connector, key)))
            await delete_object(connector, key)

    bad_id = None
    output_keys = []
    handler = ListHandler()
    poll_logger = logging.getLogger("app.services.s3_ingestion")
    poll_logger.addHandler(handler)
    poll_logger.setLevel(logging.INFO)

    try:
        bad_config = dict(connector.connection_config or {})
        bad_config["secret_key"] = "wrong-secret"
        async with AsyncSessionLocal_app() as session:
            bad = Connector(
                type=ConnectorType.S3,
                api_key=f"poll-bad-{RUN}",
                api_secret="poll-bad-secret",
                is_active=True,
                org_id=connector.org_id,
                field_mapping=list(connector.field_mapping or []),
                connection_config=bad_config,
                notification_email="test@example.com",
            )
            session.add(bad)
            await session.commit()
            await session.refresh(bad)
            bad_id = bad.id

        await put_object_bytes(connector, csv_key, MIXED)
        await put_object_bytes(connector, txt_key, TXT_BODY)

        first = await poll_once()
        print(f"first poll {first}")
        assert first["files_processed"] == 1, first
        assert first["files_skipped"] == 1, first
        assert first["connectors_failed"] >= 1, first
        assert csv_key not in await list_keys(connector, "ingestion")
        assert await get_object_bytes(connector, txt_key) == TXT_BODY

        logged = " ".join(record.getMessage() for record in handler.records)
        assert str(bad_id) in logged, logged

        batch = await latest_batch(csv_key)
        assert batch is not None
        assert batch.success_leads == 2, batch.success_leads
        assert batch.failed_leads == 2, batch.failed_leads
        success_key = folder_key(connector, "success", f"{batch.id}_{CSV_NAME}")
        failure_key = folder_key(connector, "failure", f"{batch.id}_{CSV_NAME}")
        output_keys.extend([success_key, failure_key])
        success_fields, success_rows = parse_csv(await get_object_bytes(connector, success_key))
        failure_fields, failure_rows = parse_csv(await get_object_bytes(connector, failure_key))
        assert success_fields == ["customer_id", "phone_number"]
        assert [row["customer_id"] for row in success_rows] == [GOOD_A, GOOD_B]
        assert failure_fields[-1] == "failure_reason"
        assert len(failure_rows) == 2
        print("first poll ok")

        async with AsyncSessionLocal_app() as session:
            stale = await session.get(Connector, bad_id)
            if stale is not None:
                await session.delete(stale)
                await session.commit()
        bad_id = None

        second = await poll_once()
        print(f"second poll {second}")
        assert second["files_processed"] == 0, second
        assert second["connectors_failed"] == 0, second
        assert await get_object_bytes(connector, txt_key) == TXT_BODY
        print("second poll ok")
        print("ok")
    finally:
        poll_logger.removeHandler(handler)
        await delete_object(connector, csv_key)
        await delete_object(connector, txt_key)
        for key in output_keys:
            await delete_object(connector, key)
        if bad_id is not None:
            async with AsyncSessionLocal_app() as session:
                stale = await session.get(Connector, bad_id)
                if stale is not None:
                    await session.delete(stale)
                    await session.commit()
        async with AsyncSessionLocal() as session:
            await session.execute(
                delete(Leads).where(
                    Leads.org_id == connector.org_id,
                    Leads.customer_id.in_([GOOD_A, GOOD_B]),
                )
            )
            await session.execute(
                delete(LeadIngestionBatch).where(LeadIngestionBatch.source_ref == csv_key)
            )
            await session.commit()
        for key, payload in preserved:
            await put_object_bytes(connector, key, payload)
        print("cleaned up")


if __name__ == "__main__":
    asyncio.run(main())
