import csv

from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.users import Users
from app.models.clients import ClientStatus, Clients
from app.dependencies import require_role, get_db, get_leads_db
from app.org_id import is_ten_digit_org_id
from app.models.leads import LeadSource
from app.services.ingestion import (
    batch_summary,
    complete_batch,
    fail_batch,
    open_batch,
)

router = APIRouter()


async def _require_active_org(db: AsyncSession, org_id: int) -> None:
    result = await db.execute(select(Clients).where(Clients.id == org_id))
    client = result.scalar_one_or_none()
    if client is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="org_id does not match a client.",
        )
    if client.status != ClientStatus.ACTIVE:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Client is inactive.",
        )


@router.post("/leads/upload")
async def upload_leads(
    file: UploadFile = File(...),
    org_id: int | None = Form(None),
    current_user: Users = Depends(require_role(["ADMIN", "ORG_ADMIN"])),
    app_db: AsyncSession = Depends(get_db),
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

    await _require_active_org(app_db, target_org_id)

    batch = await open_batch(
        leads_db,
        org_id=target_org_id,
        source=LeadSource.CSV,
        source_ref=file.filename,
    )

    try:
        content = await file.read()
        reader = csv.DictReader(content.decode("utf-8").splitlines())
        rows = list(reader)
    except UnicodeDecodeError:
        await fail_batch(leads_db, batch, "File is not valid UTF-8 text.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File is not valid UTF-8 text.",
        )

    if not rows:
        await fail_batch(leads_db, batch, "No data found in the uploaded file.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No data found in the uploaded file.",
        )

    batch, success_rows, error_log = await complete_batch(
        leads_db,
        batch,
        rows,
        target_org_id,
        LeadSource.CSV,
    )

    return batch_summary(
        batch,
        rows,
        success_rows,
        error_log,
        target_org_id=target_org_id,
    )
