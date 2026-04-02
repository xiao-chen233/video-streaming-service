from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from sqlalchemy import select

from app.api.schemas import (
    BatchStartRequest,
    BatchStopRequest,
    StartStreamRequest,
    StandardResponse,
    StopStreamRequest,
    StreamCatalogItem,
    StreamStatusResponse,
)
from app.core.database import SessionLocal
from app.models.stream import Stream


router = APIRouter(prefix="/streams", tags=["streams"])


@router.post("/start", response_model=StreamStatusResponse)
async def start_stream(request: Request, payload: StartStreamRequest) -> StreamStatusResponse:
    manager = request.app.state.stream_manager
    producer = getattr(request.app.state, "kafka_producer", None)
    try:
        if producer:
            from app.api.schemas import CommandMessage

            await producer.publish(
                CommandMessage(action="start", stream_id=payload.stream_id, url=payload.url, output=payload.output_dir)
            )
        return await manager.start_stream(payload)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/stop", response_model=StreamStatusResponse)
async def stop_stream(request: Request, payload: StopStreamRequest) -> StreamStatusResponse:
    manager = request.app.state.stream_manager
    producer = getattr(request.app.state, "kafka_producer", None)
    try:
        if producer:
            from app.api.schemas import CommandMessage

            await producer.publish(CommandMessage(action="stop", stream_id=payload.stream_id))
        return await manager.stop_stream(payload.stream_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/{stream_id}", response_model=StandardResponse)
async def delete_stream(request: Request, stream_id: str, purge_files: bool = False) -> StandardResponse:
    manager = request.app.state.stream_manager
    try:
        message = await manager.delete_stream(stream_id=stream_id, purge_files=purge_files)
        return StandardResponse(ok=True, message=message)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/status/{stream_id}", response_model=StreamStatusResponse)
async def stream_status(request: Request, stream_id: str) -> StreamStatusResponse:
    manager = request.app.state.stream_manager
    return await manager.get_status(stream_id)


@router.get("/list", response_model=list[StreamStatusResponse])
async def stream_list(request: Request) -> list[StreamStatusResponse]:
    manager = request.app.state.stream_manager
    return await manager.list_status()


@router.get("/catalog", response_model=list[StreamCatalogItem])
async def stream_catalog() -> list[StreamCatalogItem]:
    with SessionLocal() as session:
        rows = session.scalars(select(Stream).order_by(Stream.updated_at.desc())).all()
    return [
        StreamCatalogItem(
            stream_id=row.id,
            url=row.url,
            output_dir=row.output_dir,
            status=row.status,
            node_id=row.node_id,
            updated_at=row.updated_at.isoformat(),
        )
        for row in rows
    ]


@router.post("/start/batch", response_model=list[StreamStatusResponse])
async def start_batch(request: Request, payload: BatchStartRequest) -> list[StreamStatusResponse]:
    manager = request.app.state.stream_manager
    responses: list[StreamStatusResponse] = []
    for item in payload.streams:
        responses.append(await manager.start_stream(item))
    return responses


@router.post("/stop/batch", response_model=list[StreamStatusResponse])
async def stop_batch(request: Request, payload: BatchStopRequest) -> list[StreamStatusResponse]:
    manager = request.app.state.stream_manager
    responses: list[StreamStatusResponse] = []
    for stream_id in payload.stream_ids:
        responses.append(await manager.stop_stream(stream_id))
    return responses


@router.get("/healthz", response_model=StandardResponse)
async def healthz() -> StandardResponse:
    return StandardResponse(ok=True, message="ok")
