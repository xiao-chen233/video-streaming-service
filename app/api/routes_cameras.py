from __future__ import annotations

from fastapi import APIRouter, Query
from sqlalchemy import select

from app.api.schemas import CameraInfoItem
from app.core.database import SessionLocal
from app.models.camera_info import CameraInfo


router = APIRouter(prefix="/cameras", tags=["cameras"])


@router.get("", response_model=list[CameraInfoItem])
async def camera_info_list(
    keyword: str | None = None,
    monitoring_type: str | None = None,
    subsidiary_company_id: int | None = None,
    grassroots_enterprises_id: int | None = None,
    discharge_port_id: int | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> list[CameraInfoItem]:
    conditions = []
    if keyword:
        like_kw = f"%{keyword.strip()}%"
        conditions.append(
            (CameraInfo.camera_name.like(like_kw))
            | (CameraInfo.camera_gb_code.like(like_kw))
            | (CameraInfo.discharge_port_name.like(like_kw))
            | (CameraInfo.discharge_port_number.like(like_kw))
        )
    if monitoring_type:
        conditions.append(CameraInfo.monitoring_type == monitoring_type)
    if subsidiary_company_id is not None:
        conditions.append(CameraInfo.subsidiary_company_id == subsidiary_company_id)
    if grassroots_enterprises_id is not None:
        conditions.append(CameraInfo.grassroots_enterprises_id == grassroots_enterprises_id)
    if discharge_port_id is not None:
        conditions.append(CameraInfo.discharge_port_id == discharge_port_id)

    stmt = select(CameraInfo)
    if conditions:
        stmt = stmt.where(*conditions)
    stmt = stmt.order_by(CameraInfo.id.desc()).offset(offset).limit(limit)

    with SessionLocal() as session:
        rows = session.scalars(stmt).all()

    return [
        CameraInfoItem(
            id=row.id,
            subsidiary_company=row.subsidiary_company,
            subsidiary_company_id=row.subsidiary_company_id,
            subsidiary_company_code=row.subsidiary_company_code,
            grassroots_enterprises=row.grassroots_enterprises,
            grassroots_enterprises_code=row.grassroots_enterprises_code,
            camera_name=row.camera_name,
            camera_gb_code=row.camera_gb_code,
            discharge_port_id=row.discharge_port_id,
            discharge_port_name=row.discharge_port_name,
            discharge_port_number=row.discharge_port_number,
            monitoring_type=row.monitoring_type,
            grassroots_enterprises_id=row.grassroots_enterprises_id,
        )
        for row in rows
    ]
