from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.leads import Leads


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