from __future__ import annotations

from sqlalchemy import or_, select
from sqlalchemy.exc import SQLAlchemyError

from app.core.database import SessionLocal


class CameraInfoRepository:
    def find_camera_gb_code(self, stream_id: str) -> str | None:
        from app.models.camera_info import CameraInfo

        conditions = [
            CameraInfo.camera_gb_code == stream_id,
            CameraInfo.discharge_port_number == stream_id,
            CameraInfo.discharge_port_name == stream_id,
        ]
        try:
            stream_id_as_int = int(stream_id)
            conditions.append(CameraInfo.discharge_port_id == stream_id_as_int)
        except ValueError:
            pass

        stmt = select(CameraInfo.camera_gb_code).where(or_(*conditions)).order_by(CameraInfo.id).limit(1)
        try:
            with SessionLocal() as session:
                value = session.scalar(stmt)
                if value is None:
                    return None
                return str(value)
        except SQLAlchemyError as exc:
            raise RuntimeError(f"failed to query camera info table: {exc}") from exc
