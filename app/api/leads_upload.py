from datetime import datetime, timezone
import csv

from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from app.models.users import Users
from app.dependencies import require_role, get_leads_db
from app.org_id import is_ten_digit_org_id
from app.models.lead_ingestion_batch import LeadIngestionBatch, LeadIngestionBatchStatus
from app.models.leads import LeadSource, LeadStatus, Leads
from app.utils.normalization import compute_dedup_hash, normalize_customer_id, normalize_phone_number
from app.services.ingestion import flush_pending

router = APIRouter()

BATCH_SIZE = 500

@router.post("/leads/upload")
async def upload_leads(
    file: UploadFile = File(...),
    org_id: int | None = Form(None),
    current_user: Users = Depends(require_role(["ADMIN", "ORG_ADMIN"])),
    leads_db: AsyncSession = Depends(get_leads_db),
):
    if current_user.org_id is not None:
        target_org_id = current_user.org_id
    else:
        if org_id is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Internal users must specify an org_id for the upload.",
            )
        if not is_ten_digit_org_id(org_id):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="org_id must be a 10-digit number.",
            )
        target_org_id = org_id

    batch = LeadIngestionBatch(
        org_id=target_org_id,
        connector_id=None,
        status=LeadIngestionBatchStatus.PROCESSING,
        source=LeadSource.CSV,
        source_ref=file.filename,
    )
    leads_db.add(batch)
    await leads_db.commit()
    await leads_db.refresh(batch)
    batch_id = batch.id

    try:
        content = await file.read()
        reader = csv.DictReader(content.decode("utf-8").splitlines())
        rows = list(reader)
    except UnicodeDecodeError:
        batch.status = LeadIngestionBatchStatus.FAILED
        batch.error_messages = [{"reason": "File is not valid UTF-8 text."}]
        batch.completed_at = datetime.now(timezone.utc)
        await leads_db.commit()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File is not valid UTF-8 text.",
        )

    if not rows:
        batch.status = LeadIngestionBatchStatus.FAILED
        batch.total_leads = 0
        batch.error_messages = [{"reason": "No data found in the uploaded file."}]
        batch.completed_at = datetime.now(timezone.utc)
        await leads_db.commit()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No data found in the uploaded file.",
        )

    pending_leads: list[tuple[int, Leads]] = []
    success_rows: list[dict] = []
    error_log: list[dict] = []
    seen_customer_ids: set[str] = set()
    seen_hashes: set[str] = set()

    for i, row in enumerate(rows, start=1):
        try:
            customer_id = normalize_customer_id(row.get("customer_id"))
            phone_number = normalize_phone_number(row.get("phone_number"))
        except ValueError as exc:
            error_log.append({"row_number": i, "reason": str(exc)})
            continue

        if customer_id in seen_customer_ids:
            error_log.append({"row_number": i, "reason": "Duplicate customer_id in file"})
            continue

        dedup_hash = compute_dedup_hash(target_org_id, customer_id, phone_number)
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
            source=LeadSource.CSV,
            idempotency_key=None,
            dedup_hash=dedup_hash,
            lead_attributes={
                key: value
                for key, value in row.items()
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
        "total_rows_parsed": len(rows),
        "total_rows_success": len(success_rows),
        "total_rows_error": len(error_log),
        "error_log": error_log,
        "success_rows": success_rows,
    }
