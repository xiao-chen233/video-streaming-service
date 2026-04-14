from sqlalchemy import BigInteger, String
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class CameraInfo(Base):
    __tablename__ = "t_lk_monitor_camera_info"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    subsidiary_company: Mapped[str | None] = mapped_column(String(50), nullable=True)
    subsidiary_company_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    subsidiary_company_code: Mapped[str | None] = mapped_column(String(50), nullable=True)
    grassroots_enterprises: Mapped[str | None] = mapped_column(String(50), nullable=True)
    grassroots_enterprises_code: Mapped[str | None] = mapped_column(String(50), nullable=True)
    camera_name: Mapped[str | None] = mapped_column(String(50), nullable=True)
    camera_gb_code: Mapped[str | None] = mapped_column(String(50), nullable=True)
    discharge_port_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    discharge_port_name: Mapped[str | None] = mapped_column(String(50), nullable=True)
    discharge_port_number: Mapped[str | None] = mapped_column(String(50), nullable=True)
    monitoring_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    grassroots_enterprises_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
