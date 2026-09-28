from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    Application settings.
    """

    leads_db_url: str
    app_config_db_url: str
    jwt_secret_key: str
    jwt_algorithm: str
    jwt_access_token_expire_minutes: int

    minio_endpoint_url: str
    minio_root_user: str
    minio_root_password: str
    minio_bucket_name: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()