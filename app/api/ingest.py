import csv
from datetime import datetime, timezone

from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from app.config import settings
from app.dependencies import get_current_connector, get_leads_db
from app.models.connector import Connector
from app.models.lead_ingestion_batch import LeadIngestionBatch, LeadIngestionBatchStatus
from app.models.leads import Leads, LeadSource, LeadStatus
from app.utils.normalization import translate_payload, compute_dedup_hash
from app.services.ingestion import flush_pending
from app.services.s3_client import get_s3_client

router = APIRouter()

BATCH_SIZE = 100


class S3IngestRequest(BaseModel):
    object_key: str


def _fetch_s3_object(object_key: str) -> bytes:
    """Blocking boto3 download - always call via run_in_threadpool from an
    async endpoint."""
    client = get_s3_client()
    try:
        response = client.get_object(Bucket=settings.minio_bucket_name, Key=object_key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in ("NoSuchKey", "404"):
            raise FileNotFoundError(object_key) from exc
        raise
    return response["Body"].read()


@router.post("/leads/ingest/{connector_id}")
async def ingest_leads(
    payload: dict,
    connector: Connector = Depends(get_current_connector),
    leads_db: AsyncSession = Depends(get_leads_db),
):
    mapped_targets = {
        mapping.get("lead_attribute")
        for mapping in (connector.field_mapping or [])
    }
    if "customer_id" not in mapped_targets or "phone_number" not in mapped_targets:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Connector field_mapping must map both customer_id and phone_number.",
        )

    raw_leads = payload.get("data", [])
    if not isinstance(raw_leads, list):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="payload.data must be a list of leads",
        )

    batch = LeadIngestionBatch(
        org_id=connector.org_id,
        connector_id=connector.id,
        status=LeadIngestionBatchStatus.PROCESSING,
        source=LeadSource.API,
        source_ref=None,
    )
    leads_db.add(batch)
    await leads_db.commit()
    await leads_db.refresh(batch)
    batch_id = batch.id

    if not raw_leads:
        batch.status = LeadIngestionBatchStatus.FAILED
        batch.total_leads = 0
        batch.error_messages = [{"reason": "No data found in the payload."}]
        batch.completed_at = datetime.now(timezone.utc)
        await leads_db.commit()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No data found in the payload.",
        )

    pending_leads: list[tuple[int, Leads]] = []
    success_rows: list[dict] = []
    error_log: list[dict] = []
    seen_customer_ids: set[str] = set()
    seen_hashes: set[str] = set()
    target_org_id = connector.org_id

    for i, raw_lead in enumerate(raw_leads, start=1):
        if not isinstance(raw_lead, dict):
            error_log.append({"row_number": i, "reason": "Lead payload must be an object"})
            continue

        try:
            translated = translate_payload(raw_lead, connector.field_mapping)
            customer_id = translated.get("customer_id")
            phone_number = translated.get("phone_number")
            if not customer_id or not phone_number:
                raise ValueError("customer_id and phone_number are required")
            dedup_hash = compute_dedup_hash(target_org_id, customer_id, phone_number)
        except ValueError as exc:
            error_log.append({"row_number": i, "reason": str(exc)})
            continue

        if customer_id in seen_customer_ids:
            error_log.append({"row_number": i, "reason": "Duplicate customer_id in payload"})
            continue

        if dedup_hash in seen_hashes:
            error_log.append({"row_number": i, "reason": "Duplicate found"})
            continue

        existing_hash = await leads_db.execute(
            select(Leads).where(
                Leads.org_id == target_org_id,
                Leads.dedup_hash == dedup_hash,
            )
        )
        if existing_hash.scalar_one_or_none():
            error_log.append({"row_number": i, "reason": "Duplicate found"})
            continue

        existing_customer = await leads_db.execute(
            select(Leads).where(
                Leads.org_id == target_org_id,
                Leads.customer_id == customer_id,
            )
        )
        if existing_customer.scalar_one_or_none():
            error_log.append({"row_number": i, "reason": "Duplicate customer_id"})
            continue

        lead = Leads(
            org_id=target_org_id,
            customer_id=customer_id,
            phone_number=phone_number,
            source=LeadSource.API,
            dedup_hash=dedup_hash,
            lead_attributes={
                key: value
                for key, value in translated.items()
                if key not in ("customer_id", "phone_number")
            },
            status=LeadStatus.PENDING,
        )
        leads_db.add(lead)
        pending_leads.append((i, lead))
        seen_customer_ids.add(customer_id)
        seen_hashes.add(dedup_hash)

        if len(pending_leads) >= BATCH_SIZE:
            await flush_pending(leads_db, pending_leads, success_rows, error_log)
            pending_leads = []

    await flush_pending(leads_db, pending_leads, success_rows, error_log)

    batch = await leads_db.get(LeadIngestionBatch, batch_id)
    batch.total_leads = len(raw_leads)
    batch.success_leads = len(success_rows)
    batch.failed_leads = len(error_log)
    batch.error_messages = error_log
    batch.completed_at = datetime.now(timezone.utc)
    if error_log:
        batch.status = LeadIngestionBatchStatus.COMPLETED_WITH_ERRORS
    else:
        batch.status = LeadIngestionBatchStatus.COMPLETED_SUCCESSFULLY

    await leads_db.commit()
    await leads_db.refresh(batch)

    return {
        "batch_id": str(batch.id),
        "status": batch.status,
        "target_org_id": target_org_id,
        "total_rows_parsed": len(raw_leads),
        "total_rows_success": len(success_rows),
        "total_rows_error": len(error_log),
        "error_log": error_log,
        "success_rows": success_rows,
    }


@router.post("/leads/ingest/{connector_id}/s3")
async def ingest_leads_from_s3(
    body: S3IngestRequest,
    connector: Connector = Depends(get_current_connector),
    leads_db: AsyncSession = Depends(get_leads_db),
):
    mapped_targets = {
        mapping.get("lead_attribute")
        for mapping in (connector.field_mapping or [])
    }
    if "customer_id" not in mapped_targets or "phone_number" not in mapped_targets:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Connector field_mapping must map both customer_id and phone_number.",
        )

    try:
        raw_bytes = await run_in_threadpool(_fetch_s3_object, body.object_key)
    except FileNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object '{body.object_key}' not found in bucket '{settings.minio_bucket_name}'.",
        )

    batch = LeadIngestionBatch(
        org_id=connector.org_id,
        connector_id=connector.id,
        status=LeadIngestionBatchStatus.PROCESSING,
        source=LeadSource.S3,
        source_ref=body.object_key,
    )
    leads_db.add(batch)
    await leads_db.commit()
    await leads_db.refresh(batch)
    batch_id = batch.id

    try:
        rows = list(csv.DictReader(raw_bytes.decode("utf-8").splitlines()))
    except UnicodeDecodeError:
        batch.status = LeadIngestionBatchStatus.FAILED
        batch.error_messages = [{"reason": "Object is not valid UTF-8 CSV text."}]
        batch.completed_at = datetime.now(timezone.utc)
        await leads_db.commit()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Object is not valid UTF-8 CSV text.",
        )

    if not rows:
        batch.status = LeadIngestionBatchStatus.FAILED
        batch.total_leads = 0
        batch.error_messages = [{"reason": "No data found in the object."}]
        batch.completed_at = datetime.now(timezone.utc)
        await leads_db.commit()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No data found in the object.",
        )

    pending_leads: list[tuple[int, Leads]] = []
    success_rows: list[dict] = []
    error_log: list[dict] = []
    seen_customer_ids: set[str] = set()
    seen_hashes: set[str] = set()
    target_org_id = connector.org_id

    for i, row in enumerate(rows, start=1):
        try:
            translated = translate_payload(row, connector.field_mapping)
            customer_id = translated.get("customer_id")
            phone_number = translated.get("phone_number")
            if not customer_id or not phone_number:
                raise ValueError("customer_id and phone_number are required")
            dedup_hash = compute_dedup_hash(target_org_id, customer_id, phone_number)
        except ValueError as exc:
            error_log.append({"row_number": i, "reason": str(exc)})
            continue

        if customer_id in seen_customer_ids:
            error_log.append({"row_number": i, "reason": "Duplicate customer_id in file"})
            continue

        if dedup_hash in seen_hashes:
            error_log.append({"row_number": i, "reason": "Duplicate found"})
            continue

        existing_hash = await leads_db.execute(
            select(Leads).where(
                Leads.org_id == target_org_id,
                Leads.dedup_hash == dedup_hash,
            )
        )
        if existing_hash.scalar_one_or_none():
            error_log.append({"row_number": i, "reason": "Duplicate found"})
            continue

        existing_customer = await leads_db.execute(
            select(Leads).where(
                Leads.org_id == target_org_id,
                Leads.customer_id == customer_id,
            )
        )
        if existing_customer.scalar_one_or_none():
            error_log.append({"row_number": i, "reason": "Duplicate customer_id"})
            continue

        lead = Leads(
            org_id=target_org_id,
            customer_id=customer_id,
            phone_number=phone_number,
            source=LeadSource.S3,
            dedup_hash=dedup_hash,
            lead_attributes={
                key: value
                for key, value in translated.items()
                if key not in ("customer_id", "phone_number")
            },
            status=LeadStatus.PENDING,
        )
        leads_db.add(lead)
        pending_leads.append((i, lead))
        seen_customer_ids.add(customer_id)
        seen_hashes.add(dedup_hash)

        if len(pending_leads) >= BATCH_SIZE:
            await flush_pending(leads_db, pending_leads, success_rows, error_log)
            pending_leads = []

    await flush_pending(leads_db, pending_leads, success_rows, error_log)

    batch = await leads_db.get(LeadIngestionBatch, batch_id)
    batch.total_leads = len(rows)
    batch.success_leads = len(success_rows)
    batch.failed_leads = len(error_log)
    batch.error_messages = error_log
    batch.completed_at = datetime.now(timezone.utc)
    if error_log:
        batch.status = LeadIngestionBatchStatus.COMPLETED_WITH_ERRORS
    else:
        batch.status = LeadIngestionBatchStatus.COMPLETED_SUCCESSFULLY

    await leads_db.commit()
    await leads_db.refresh(batch)

    return {
        "batch_id": str(batch.id),
        "status": batch.status,
        "target_org_id": target_org_id,
        "object_key": body.object_key,
        "total_rows_parsed": len(rows),
        "total_rows_success": len(success_rows),
        "total_rows_error": len(error_log),
        "error_log": error_log,
        "success_rows": success_rows,
    }
