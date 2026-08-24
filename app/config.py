from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Application settings.
    """

    leads_db_url: str
    app_config_db_url: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()