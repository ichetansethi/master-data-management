from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, status
import csv

from app.models.users import Users
from app.dependencies import require_role
from app.org_id import is_ten_digit_org_id

router = APIRouter()


@router.post("/leads/upload")
async def upload_leads(
    file: UploadFile = File(...),
    org_id: int | None = Form(None),
    current_user: Users = Depends(require_role(["ADMIN", "ORG_ADMIN"])),
):
    if current_user.org_id is not None:
        # client-scoped user (ORG_ADMIN) — always use their own org, ignore any org_id passed in
        target_org_id = current_user.org_id
    else:
        # internal GreyLabs user (ADMIN) — must specify which org this upload is for
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

    rows = []

    try:
        content = await file.read()
        reader = csv.DictReader(content.decode('utf-8').splitlines())
        rows = list(reader)
    except UnicodeDecodeError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File is not valid UTF-8 text.")

    if not rows:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No data found in the uploaded file.")

    return {"total_rows": len(rows), "rows": rows, "target_org_id": target_org_id}
