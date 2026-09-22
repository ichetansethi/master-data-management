from fastapi import APIRouter, Depends
import uuid

from app.dependencies import get_current_connector
from app.models.connector import Connector

router = APIRouter()

@router.post("/leads/ingest/{connector_id}")
async def ingest_lead(
    connector_id: uuid.UUID,
    connector: Connector = Depends(get_current_connector),
):
    return {
        "connector_id": str(connector.id),
        "org_id": connector.org_id,
        "message": "Connector authenticated successfully",
    }