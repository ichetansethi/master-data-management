from fastapi import FastAPI

app = FastAPI(title="Call Reporting Platform")


@app.get("/health")
async def health_check():
    return {"status": "ok"}