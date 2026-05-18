from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    port: int = Field(alias="PRODUCER_PORT")
    reload_app: bool = Field(alias="RELOAD")

    kafka_bootstrap_servers: str = Field(alias="KAFKA_INTERNAL_BOOTSTRAP_SERVERS")
    kafka_client_id: str = Field(alias="KAFKA_PRODUCER_CLIENT_ID")
    kafka_price_change_topic: str = Field(alias="KAFKA_PRICE_CHANGE_TOPIC")
    kafka_idempotence: bool = Field(alias="KAFKA_IDEMPOTENCE")

    model_config = SettingsConfigDict(
        extra="ignore",
    )


settings = Settings()