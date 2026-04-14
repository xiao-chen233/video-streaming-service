from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.core.config import get_settings


class Base(DeclarativeBase):
    pass


settings = get_settings()
engine = create_engine(settings.db_url, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def init_db() -> None:
    from app.models.record_file import RecordFile
    from app.models.stream import Stream

    Stream.__table__.create(bind=engine, checkfirst=True)
    RecordFile.__table__.create(bind=engine, checkfirst=True)
