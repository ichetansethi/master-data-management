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

    s3_endpoint_url: str
    s3_poll_interval_seconds: int = 60

    kafka_bootstrap_servers: str = "localhost:9092"
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = "clickhouse"
    clickhouse_database: str = "call_reporting"
    kafka_call_outcomes_topic: str = "call-outcomes"
    kafka_call_outcomes_partitions: int = 3
    kafka_call_outcomes_dlq_topic: str = "call-outcomes-dlq"
    kafka_consumer_group: str = "call-outcomes-clickhouse"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()