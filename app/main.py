from contextlib import asynccontextmanager

from clickhouse_connect.driver.asyncclient import AsyncClient
from fastapi import Depends, FastAPI

from app.api.ingest import router as ingest_router
from app.api.leads_upload import router as leads_router
from app.clickhouse import close_clickhouse, connect_clickhouse
from app.dependencies import get_clickhouse
from app.login import router as login_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect_clickhouse()
    try:
        yield
    finally:
        await close_clickhouse()


app = FastAPI(title="Call Reporting Platform", lifespan=lifespan)
app.include_router(login_router)
app.include_router(leads_router)
app.include_router(ingest_router)

@app.get("/health")
async def health_check(clickhouse: AsyncClient = Depends(get_clickhouse)):
    await clickhouse.command("SELECT 1")
    return {"status": "ok"}
