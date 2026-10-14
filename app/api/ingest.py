import csv

from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_current_connector, get_leads_db
from app.models.connector import Connector
from app.models.leads import LeadSource
from app.services.ingestion import (
    TooManyLeads,
    batch_summary,
    complete_batch,
    fail_batch,
    open_batch,
)
from app.services.s3_client import get_s3_client

router = APIRouter()


class S3IngestRequest(BaseModel):
    object_key: str


def _resolve_connection_config(connection_config: dict | None) -> dict:
    """Validate a connector's connection_config has what's needed to reach
    its bucket, and return it. Raises ValueError (-> 400) if misconfigured."""
    connection_config = connection_config or {}
    missing = [
        field
        for field in ("bucket", "access_key", "secret_key")
        if not connection_config.get(field)
    ]
    if missing:
        raise ValueError(
            "Connector connection_config is missing required field(s): "
            + ", ".join(missing)
        )
    return connection_config


def _resolve_object_key(connection_config: dict, object_key: str) -> str:
    prefix = connection_config.get("prefix") or ""
    if not prefix:
        return object_key
    return prefix.rstrip("/") + "/" + object_key.lstrip("/")


def _require_identity_mapping(connector: Connector) -> None:
    mapped_targets = {
        mapping.get("lead_attribute")
        for mapping in (connector.field_mapping or [])
    }
    if "customer_id" not in mapped_targets or "phone_number" not in mapped_targets:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Connector field_mapping must map both customer_id and phone_number.",
        )


async def _fetch_s3_object(connection_config: dict, object_key: str) -> bytes:
    """Download one object with the connector's own bucket + credentials.
    Each connector owns its own bucket (connection_config), not a shared one."""
    full_key = _resolve_object_key(connection_config, object_key)
    async with get_s3_client(
        access_key=connection_config["access_key"],
        secret_key=connection_config["secret_key"],
        endpoint_url=connection_config.get("endpoint_url"),
    ) as client:
        try:
            response = await client.get_object(
                Bucket=connection_config["bucket"],
                Key=full_key,
            )
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code in ("NoSuchKey", "404"):
                raise FileNotFoundError(full_key) from exc
            raise
        return await response["Body"].read()


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


@router.post("/leads/ingest/{connector_id}/s3")
async def ingest_leads_from_s3(
    body: S3IngestRequest,
    connector: Connector = Depends(get_current_connector),
    leads_db: AsyncSession = Depends(get_leads_db),
):
    _require_identity_mapping(connector)

    try:
        connection_config = _resolve_connection_config(connector.connection_config)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        )

    full_key = _resolve_object_key(connection_config, body.object_key)

    try:
        raw_bytes = await _fetch_s3_object(connection_config, body.object_key)
    except FileNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object '{full_key}' not found in bucket '{connection_config['bucket']}'.",
        )

    batch = await open_batch(
        leads_db,
        org_id=connector.org_id,
        connector_id=connector.id,
        source=LeadSource.S3,
        source_ref=full_key,
    )

    try:
        rows = list(csv.DictReader(raw_bytes.decode("utf-8").splitlines()))
    except UnicodeDecodeError:
        await fail_batch(leads_db, batch, "Object is not valid UTF-8 CSV text.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Object is not valid UTF-8 CSV text.",
        )

    if not rows:
        await fail_batch(leads_db, batch, "No data found in the object.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No data found in the object.",
        )

    try:
        batch, success_rows, error_log = await complete_batch(
            leads_db,
            batch,
            rows,
            connector.org_id,
            LeadSource.S3,
            field_mapping=connector.field_mapping,
        )
    except TooManyLeads as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        )

    return batch_summary(
        batch,
        rows,
        success_rows,
        error_log,
        target_org_id=connector.org_id,
        object_key=body.object_key,
        bucket=connection_config["bucket"],
        resolved_key=full_key,
    )
