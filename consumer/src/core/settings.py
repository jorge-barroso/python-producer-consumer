from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    bootstrap_servers: str = Field(alias='KAFKA_INTERNAL_BOOTSTRAP_SERVERS')
    topic: str = Field(alias='KAFKA_PRICE_CHANGE_TOPIC')
    consumer_group: str = Field(alias='KAFKA_CONSUMER_CLIENT_ID')

    model_config = SettingsConfigDict(extra="ignore")


settings = Settings()