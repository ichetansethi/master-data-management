from fastapi import FastAPI

from app.login import router as login_router
from app.api.leads_upload import router as leads_router

app = FastAPI(title="Call Reporting Platform")
app.include_router(login_router)
app.include_router(leads_router)

@app.get("/health")
async def health_check():
    return {"status": "ok"}