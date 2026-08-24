from app.config import settings
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

engine_app: AsyncEngine = create_async_engine(settings.app_config_db_url, echo=True)
AsyncSessionLocal_app = async_sessionmaker(bind=engine_app, expire_on_commit=False)

class BaseApp(DeclarativeBase):
    pass