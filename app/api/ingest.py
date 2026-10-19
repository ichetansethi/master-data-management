from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_current_connector, get_leads_db
from app.models.connector import Connector
from app.models.leads import LeadSource
from app.services.ingestion import (
    API_MAX_LEADS,
    TooManyLeads,
    batch_summary,
    complete_batch,
    fail_batch,
    open_batch,
    require_identity_mapping,
)

router = APIRouter()


def _require_identity_mapping(connector: Connector) -> None:
    try:
        require_identity_mapping(connector)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.post("/leads/ingest/{connector_id}")
async def ingest_leads(
    payload: dict,
    connector: Connector = Depends(get_current_connector),
    leads_db: AsyncSession = Depends(get_leads_db),
):
    _require_identity_mapping(connector)

    raw_leads = payload.get("data", [])
    if not isinstance(raw_leads, list):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="payload.data must be a list of leads",
        )

    batch = await open_batch(
        leads_db,
        org_id=connector.org_id,
        connector_id=connector.id,
        source=LeadSource.API,
    )

    if not raw_leads:
        await fail_batch(leads_db, batch, "No data found in the payload.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No data found in the payload.",
        )

    try:
        batch, success_rows, error_log = await complete_batch(
            leads_db,
            batch,
            raw_leads,
            connector.org_id,
            LeadSource.API,
            field_mapping=connector.field_mapping,
            max_leads=API_MAX_LEADS,
        )
    except TooManyLeads as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        )

    return batch_summary(
        batch,
        raw_leads,
        success_rows,
        error_log,
        target_org_id=connector.org_id,
    )
