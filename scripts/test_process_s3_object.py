import asyncio
import csv
import io
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
from app.models.lead_ingestion_batch import LeadIngestionBatch, LeadIngestionBatchStatus
from app.models.leads import Leads
from app.services.s3_client import (
    delete_object,
    folder_key,
    get_object_bytes,
    list_keys,
    put_object_bytes,
)
from app.services.s3_ingestion import process_s3_object

RUN = uuid.uuid4().hex[:8]
GOOD_A = f"s3obj-{RUN}-a"
GOOD_B = f"s3obj-{RUN}-b"
BOM_ID = f"s3obj-{RUN}-bom"

MIXED = (
    "customer_id,phone_number\n"
    f"{GOOD_A},9811100001\n"
    f"{GOOD_B},9811100002\n"
    "bad-phone,bad\n"
    ",9811100004\n"
).encode()

BOM = b"\xef\xbb\xbf" + (
    "customer_id,phone_number\n"
    f"{BOM_ID},9811100005\n"
).encode()

NOT_TEXT = b"\xff\xfe\x00"

HEADER_ONLY = b"customer_id,phone_number\n"

BULK_IDS = [f"s3obj-{RUN}-b{i:03d}" for i in range(150)]
BULK = (
    "customer_id,phone_number\n"
    + "".join(f"{customer_id},98{i:08d}\n" for i, customer_id in enumerate(BULK_IDS))
).encode()

INGESTION_NAMES = ("mixed.csv", "bom.csv", "not_text.bin", "header.csv", "bulk150.csv")


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


async def latest_batch(key: str) -> LeadIngestionBatch:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(LeadIngestionBatch)
            .where(LeadIngestionBatch.source_ref == key)
            .order_by(LeadIngestionBatch.created_at.desc())
        )
        batch = result.scalars().first()
    if batch is None:
        raise AssertionError(f"no batch for {key}")
    return batch


async def process(connector, filename: str, payload: bytes) -> LeadIngestionBatch:
    key = folder_key(connector, "ingestion", filename)
    await put_object_bytes(connector, key, payload)
    async with AsyncSessionLocal() as leads_db:
        await process_s3_object(connector, key, leads_db)
    return await latest_batch(key)


def parse_csv(data: bytes):
    reader = csv.DictReader(io.StringIO(data.decode("utf-8")))
    return list(reader.fieldnames), list(reader)


async def object_or_none(connector, key: str) -> bytes | None:
    try:
        return await get_object_bytes(connector, key)
    except FileNotFoundError:
        return None


async def main():
    connector = await load_test_connector()
    if connector is None:
        print("No active S3 test connector found (api_key=test_s3_api_key).")
        print("Run: python scripts/seed_test_connector.py")
        return

    created = []
    ingestion_keys = [folder_key(connector, "ingestion", name) for name in INGESTION_NAMES]
    print(f"connector_id={connector.id} run={RUN}")

    try:
        mixed_key = folder_key(connector, "ingestion", "mixed.csv")
        batch = await process(connector, "mixed.csv", MIXED)
        print(
            f"mixed batch={batch.id} status={batch.status} "
            f"success={batch.success_leads} failed={batch.failed_leads}"
        )
        assert batch.success_leads == 2, batch.success_leads
        assert batch.failed_leads == 2, batch.failed_leads
        assert batch.status == LeadIngestionBatchStatus.COMPLETED_WITH_ERRORS

        success_key = folder_key(connector, "success", f"{batch.id}_mixed.csv")
        failure_key = folder_key(connector, "failure", f"{batch.id}_mixed.csv")
        created.extend([success_key, failure_key])

        success_fields, success_rows = parse_csv(await get_object_bytes(connector, success_key))
        failure_fields, failure_rows = parse_csv(await get_object_bytes(connector, failure_key))
        assert success_fields == ["customer_id", "phone_number"]
        assert [row["customer_id"] for row in success_rows] == [GOOD_A, GOOD_B]
        assert failure_fields == ["customer_id", "phone_number", "failure_reason"]
        assert [row["failure_reason"] for row in failure_rows] == [
            "phone_number must be a valid 10-digit number",
            "customer_id is required but missing from payload",
        ]
        assert mixed_key not in await list_keys(connector, "ingestion")
        print("mixed ok")

        replay = await process(connector, "mixed.csv", MIXED)
        print(
            f"replay batch={replay.id} status={replay.status} "
            f"success={replay.success_leads} failed={replay.failed_leads}"
        )
        assert replay.success_leads == 0
        assert replay.failed_leads == 4
        replay_success = folder_key(connector, "success", f"{replay.id}_mixed.csv")
        replay_failure = folder_key(connector, "failure", f"{replay.id}_mixed.csv")
        created.append(replay_failure)
        assert await object_or_none(connector, replay_success) is None
        _, replay_rows = parse_csv(await get_object_bytes(connector, replay_failure))
        assert [row["failure_reason"] for row in replay_rows] == [
            "Duplicate found",
            "Duplicate found",
            "phone_number must be a valid 10-digit number",
            "customer_id is required but missing from payload",
        ]
        assert mixed_key not in await list_keys(connector, "ingestion")
        print("replay ok")

        bom_key = folder_key(connector, "ingestion", "bom.csv")
        bom_batch = await process(connector, "bom.csv", BOM)
        print(
            f"bom batch={bom_batch.id} status={bom_batch.status} "
            f"success={bom_batch.success_leads} failed={bom_batch.failed_leads}"
        )
        assert bom_batch.success_leads == 1
        assert bom_batch.failed_leads == 0
        assert bom_batch.status == LeadIngestionBatchStatus.COMPLETED_SUCCESSFULLY
        bom_success = folder_key(connector, "success", f"{bom_batch.id}_bom.csv")
        created.append(bom_success)
        _, bom_rows = parse_csv(await get_object_bytes(connector, bom_success))
        assert [row["customer_id"] for row in bom_rows] == [BOM_ID]
        assert bom_key not in await list_keys(connector, "ingestion")
        print("bom ok")

        junk_key = folder_key(connector, "ingestion", "not_text.bin")
        junk_batch = await process(connector, "not_text.bin", NOT_TEXT)
        print(f"junk batch={junk_batch.id} status={junk_batch.status}")
        assert junk_batch.status == LeadIngestionBatchStatus.FAILED
        junk_failure = folder_key(connector, "failure", f"{junk_batch.id}_not_text.bin")
        created.append(junk_failure)
        # The bytes could not be parsed, so failure/ is a copy of the original
        # and has no failure_reason column. The reason is only on the batch.
        assert await get_object_bytes(connector, junk_failure) == NOT_TEXT
        assert junk_batch.error_messages == [
            {"reason": "Object is not valid UTF-8 CSV text."}
        ]
        assert junk_key not in await list_keys(connector, "ingestion")
        print("non-text ok")

        header_key = folder_key(connector, "ingestion", "header.csv")
        header_batch = await process(connector, "header.csv", HEADER_ONLY)
        print(f"header batch={header_batch.id} status={header_batch.status}")
        assert header_batch.status == LeadIngestionBatchStatus.FAILED
        header_failure = folder_key(connector, "failure", f"{header_batch.id}_header.csv")
        created.append(header_failure)
        assert await get_object_bytes(connector, header_failure) == HEADER_ONLY
        assert header_key not in await list_keys(connector, "ingestion")
        print("header-only ok")

        bulk_key = folder_key(connector, "ingestion", "bulk150.csv")
        bulk_batch = await process(connector, "bulk150.csv", BULK)
        print(
            f"bulk batch={bulk_batch.id} status={bulk_batch.status} "
            f"success={bulk_batch.success_leads} failed={bulk_batch.failed_leads}"
        )
        assert bulk_batch.success_leads == 150
        assert bulk_batch.failed_leads == 0
        assert bulk_batch.status == LeadIngestionBatchStatus.COMPLETED_SUCCESSFULLY
        bulk_success = folder_key(connector, "success", f"{bulk_batch.id}_bulk150.csv")
        created.append(bulk_success)
        _, bulk_rows = parse_csv(await get_object_bytes(connector, bulk_success))
        assert [row["customer_id"] for row in bulk_rows] == BULK_IDS
        assert await object_or_none(
            connector,
            folder_key(connector, "failure", f"{bulk_batch.id}_bulk150.csv"),
        ) is None
        assert bulk_key not in await list_keys(connector, "ingestion")
        print("bulk150 ok")
        print("ok")
    finally:
        for key in ingestion_keys:
            await delete_object(connector, key)
        for key in created:
            await delete_object(connector, key)
        async with AsyncSessionLocal() as session:
            await session.execute(
                delete(Leads).where(
                    Leads.org_id == connector.org_id,
                    Leads.customer_id.like(f"s3obj-{RUN}-%"),
                )
            )
            await session.execute(
                delete(LeadIngestionBatch).where(
                    LeadIngestionBatch.source_ref.in_(ingestion_keys)
                )
            )
            await session.commit()
        print("cleaned up ingestion keys, output objects, leads, and batches")


if __name__ == "__main__":
    asyncio.run(main())
