import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

from app.config import settings

client: AsyncClient | None = None


async def connect_clickhouse() -> None:
    global client
    # One session accepts one query at a time. FastAPI runs requests in parallel.
    client = await clickhouse_connect.get_async_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        autogenerate_session_id=False,
    )


async def close_clickhouse() -> None:
    global client
    if client is not None:
        await client.close()
        client = None
