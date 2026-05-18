from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from consumer.src.core.settings import settings

engine = create_engine(
    f"{settings.postgres_host}:{settings.postgres_port}",
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)