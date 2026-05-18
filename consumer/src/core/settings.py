from os import path
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ENV_FILE = path.join(PROJECT_ROOT, '.env')

class Settings(BaseSettings):
    bootstrap_servers: str = Field(alias='KAFKA_INTERNAL_BOOTSTRAP_SERVERS')
    topic: str = Field(alias='KAFKA_PRICE_CHANGE_TOPIC')
    consumer_group: str = Field(alias='KAFKA_CONSUMER_CLIENT_ID')

    postgres_host: str = Field(alias='POSTGRES_HOST')
    postgres_port: int = Field(alias='POSTGRES_PORT')
    postgres_user: str = Field(alias='POSTGRES_USER')
    postgres_password: str = Field(alias='POSTGRES_PASSWORD')
    postgres_db: str = Field(alias='POSTGRES_DB')

    @property
    def postgres_url(self) -> str:
        return (
            f"postgresql+psycopg://"
            f"{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    model_config = SettingsConfigDict(env_file=ENV_FILE, extra="ignore")


settings = Settings()