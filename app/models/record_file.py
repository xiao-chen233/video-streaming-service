from datetime import datetime, timezone

from sqlalchemy import BigInteger, DateTime, ForeignKey, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class RecordFile(Base):
    __tablename__ = "t_lk_monitor_record_file"
    __table_args__ = (UniqueConstraint("stream_id", "file_path", name="uq_stream_file"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    stream_id: Mapped[str] = mapped_column(ForeignKey("t_lk_monitor_stream.id"), nullable=False, index=True)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    file_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
