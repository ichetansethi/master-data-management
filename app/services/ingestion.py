import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.lead_ingestion_batch import LeadIngestionBatch, LeadIngestionBatchStatus
from app.models.leads import LeadSource, LeadStatus, Leads
from app.utils.normalization import (
    compute_dedup_hash,
    normalize_customer_id,
    normalize_phone_number,
    translate_payload,
)
MAX_LEADS = 100
COMMIT_BATCH_SIZE = 100


class TooManyLeads(Exception):
    """Raised after the batch is marked failed because the request exceeds MAX_LEADS."""

    def __init__(self, submitted: int):
        self.submitted = submitted
        super().__init__(f"A single request can include at most {MAX_LEADS} leads.")

def _lead_payload(lead: Leads) -> dict:
    return {
        "org_id": lead.org_id,
        "customer_id": lead.customer_id,
        "phone_number": lead.phone_number,
        "source": lead.source,
        "idempotency_key": lead.idempotency_key,
        "dedup_hash": lead.dedup_hash,
        "lead_attributes": lead.lead_attributes,
        "status": lead.status,
    }


def _success_row(row_number: int, lead: Leads) -> dict:
    return {
        "row_number": row_number,
        "lead_id": str(lead.id),
        "customer_id": lead.customer_id,
        "phone_number": lead.phone_number,
        "dedup_hash": lead.dedup_hash,
    }


async def flush_pending(
    leads_db: AsyncSession,
    pending_leads: list[tuple[int, Leads]],
    success_rows: list[dict],
    error_log: list[dict],
) -> None:
    if not pending_leads:
        return

    snapshots = [(row_number, _lead_payload(lead)) for row_number, lead in pending_leads]

    try:
        await leads_db.commit()
        for row_number, lead in pending_leads:
            success_rows.append(_success_row(row_number, lead))
        return
    except IntegrityError:
        await leads_db.rollback()

    for row_number, payload in snapshots:
        retry = Leads(**payload)
        leads_db.add(retry)
        try:
            await leads_db.commit()
            success_rows.append(_success_row(row_number, retry))
        except IntegrityError:
            await leads_db.rollback()
            error_log.append(
                {
                    "row_number": row_number,
                    "reason": "Duplicate or constraint violation on insert",
                }
            )

def _extract_identity(row: dict, field_mapping: list[dict] | None) -> tuple[str, str, dict]:
    if field_mapping is None:
        customer_id = normalize_customer_id(row.get("customer_id"))
        phone_number = normalize_phone_number(row.get("phone_number"))
        attributes = {k: v for k, v in row.items() if k not in ("customer_id", "phone_number")}
    else:
        translated = translate_payload(row, field_mapping)
        customer_id = translated.get("customer_id")
        phone_number = translated.get("phone_number")
        if not customer_id or not phone_number:
            raise ValueError("customer_id and phone_number are required")
        attributes = {k: v for k, v in translated.items() if k not in ("customer_id", "phone_number")}
    return customer_id, phone_number, attributes

async def process_rows(
    leads_db: AsyncSession,
    rows: list,
    org_id: int,
    source: LeadSource,
    field_mapping: list[dict] | None = None,
    batch_size: int = COMMIT_BATCH_SIZE,
) -> tuple[list[dict], list[dict]]:
    pending_leads: list[tuple[int, Leads]] = []
    success_rows: list[dict] = []
    error_log: list[dict] = []
    seen_customer_ids: set[str] = set()
    seen_hashes: set[str] = set()

    for i, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            error_log.append({"row_number": i, "reason": "Lead payload must be an object"})
            continue

        try:
            customer_id, phone_number, attributes = _extract_identity(row, field_mapping)
            dedup_hash = compute_dedup_hash(org_id, customer_id, phone_number)
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
                Leads.org_id == org_id,
                Leads.dedup_hash == dedup_hash,
            )
        )
        if existing_hash.scalar_one_or_none():
            error_log.append({"row_number": i, "reason": "Duplicate found"})
            continue

        existing_customer = await leads_db.execute(
            select(Leads).where(
                Leads.org_id == org_id,
                Leads.customer_id == customer_id,
            )
        )
        if existing_customer.scalar_one_or_none():
            error_log.append({"row_number": i, "reason": "Duplicate customer_id"})
            continue

        lead = Leads(
            org_id=org_id,
            customer_id=customer_id,
            phone_number=phone_number,
            source=source,
            dedup_hash=dedup_hash,
            lead_attributes=attributes,
            status=LeadStatus.PENDING,
        )
        leads_db.add(lead)
        pending_leads.append((i, lead))
        seen_customer_ids.add(customer_id)
        seen_hashes.add(dedup_hash)

        if len(pending_leads) >= batch_size:
            await flush_pending(leads_db, pending_leads, success_rows, error_log)
            pending_leads = []

    await flush_pending(leads_db, pending_leads, success_rows, error_log)
    return success_rows, error_log


async def open_batch(
    leads_db: AsyncSession,
    *,
    org_id: int,
    source: LeadSource,
    connector_id: uuid.UUID | None = None,
    source_ref: str | None = None,
) -> LeadIngestionBatch:
    batch = LeadIngestionBatch(
        org_id=org_id,
        connector_id=connector_id,
        status=LeadIngestionBatchStatus.PROCESSING,
        source=source,
        source_ref=source_ref,
    )
    leads_db.add(batch)
    await leads_db.commit()
    await leads_db.refresh(batch)
    return batch


async def fail_batch(
    leads_db: AsyncSession,
    batch: LeadIngestionBatch,
    reason: str,
    *,
    total_leads: int = 0,
) -> None:
    batch.status = LeadIngestionBatchStatus.FAILED
    batch.total_leads = total_leads
    batch.error_messages = [{"reason": reason}]
    batch.completed_at = datetime.now(timezone.utc)
    await leads_db.commit()


async def complete_batch(
    leads_db: AsyncSession,
    batch: LeadIngestionBatch,
    rows: list,
    org_id: int,
    source: LeadSource,
    field_mapping: list[dict] | None = None,
) -> tuple[LeadIngestionBatch, list[dict], list[dict]]:
    if len(rows) > MAX_LEADS:
        await fail_batch(
            leads_db,
            batch,
            f"A single request can include at most {MAX_LEADS} leads.",
            total_leads=len(rows),
        )
        raise TooManyLeads(len(rows))

    success_rows, error_log = await process_rows(
        leads_db,
        rows,
        org_id,
        source,
        field_mapping=field_mapping,
        batch_size=COMMIT_BATCH_SIZE,
    )

    batch = await leads_db.get(LeadIngestionBatch, batch.id)
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
    return batch, success_rows, error_log


def batch_summary(
    batch: LeadIngestionBatch,
    rows: list,
    success_rows: list[dict],
    error_log: list[dict],
    **extra,
) -> dict:
    summary = {
        "batch_id": str(batch.id),
        "status": batch.status,
    }
    summary.update(extra)
    summary.update(
        {
            "total_rows_parsed": len(rows),
            "total_rows_success": len(success_rows),
            "total_rows_error": len(error_log),
            "error_log": error_log,
            "success_rows": success_rows,
        }
    )
    return summary
