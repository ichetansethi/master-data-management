from fastapi import FastAPI

from app.login import router as login_router

app = FastAPI(title="Call Reporting Platform")
app.include_router(login_router)


@app.get("/health")
async def health_check():
    return {"status": "ok"}