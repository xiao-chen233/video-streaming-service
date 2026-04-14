from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
import logging
import os
from pathlib import Path
import shutil

from sqlalchemy import delete, select

from app.api.schemas import StartStreamRequest, StreamStatusResponse
from app.core.config import Settings
from app.core.database import SessionLocal
from app.core.metrics import recording_active_streams, recording_errors_total, recording_restart_count
from app.models.record_file import RecordFile
from app.models.stream import Stream
from app.services.camera_info_repository import CameraInfoRepository
from app.services.camera_stream_url_service import CameraStreamUrlService
from app.services.ffmpeg_recorder import FFmpegRecorder
from app.workers.redis_lock import RedisLockManager


logger = logging.getLogger(__name__)


class StreamStatus(str, Enum):
    INIT = "INIT"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    ERROR = "ERROR"
    RESTARTING = "RESTARTING"
    CIRCUIT_OPEN = "CIRCUIT_OPEN"
    STOPPED = "STOPPED"


@dataclass
class StreamRuntime:
    recorder: FFmpegRecorder
    output_dir: str
    current_url: str
    camera_gb_code: str | None = None
    status: StreamStatus = StreamStatus.INIT
    restarts: int = 0
    last_error: str | None = None
    consecutive_failures: int = 0
    next_retry_at: datetime | None = None
    circuit_open_until: datetime | None = None


class StreamManager:
    def __init__(
        self,
        settings: Settings,
        redis_lock: RedisLockManager | None = None,
        camera_info_repo: CameraInfoRepository | None = None,
        camera_stream_service: CameraStreamUrlService | None = None,
    ):
        self.settings = settings
        self.redis_lock = redis_lock
        self.camera_info_repo = camera_info_repo or CameraInfoRepository()
        self.camera_stream_service = camera_stream_service or CameraStreamUrlService(
            api_url=settings.camera_index_api_url,
            protocol=settings.camera_index_api_protocol,
            timeout_seconds=settings.camera_index_api_timeout_seconds,
        )
        self._streams: dict[str, StreamRuntime] = {}
        self._lock = asyncio.Lock()

    def _set_metrics(self) -> None:
        active = sum(1 for x in self._streams.values() if x.status == StreamStatus.RUNNING and x.recorder.is_alive())
        recording_active_streams.set(active)

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(timezone.utc)

    def _compute_backoff_seconds(self, consecutive_failures: int) -> int:
        base = max(1, self.settings.restart_backoff_base_seconds)
        cap = max(base, self.settings.restart_backoff_max_seconds)
        backoff = base * (2 ** max(0, consecutive_failures - 1))
        return min(backoff, cap)

    @staticmethod
    def _reset_recovery_state(runtime: StreamRuntime) -> None:
        runtime.consecutive_failures = 0
        runtime.next_retry_at = None
        runtime.circuit_open_until = None

    def _upsert_stream_db(self, stream_id: str, url: str, output_dir: str, status: StreamStatus) -> None:
        with SessionLocal() as session:
            obj = session.get(Stream, stream_id)
            if obj is None:
                obj = Stream(
                    id=stream_id,
                    url=url,
                    status=status.value,
                    node_id=self.settings.node_id,
                    output_dir=output_dir,
                    updated_at=datetime.now(timezone.utc),
                )
                session.add(obj)
            else:
                obj.url = url
                obj.output_dir = output_dir
                obj.status = status.value
                obj.node_id = self.settings.node_id
                obj.updated_at = datetime.now(timezone.utc)
            session.commit()

    def _update_stream_status_db(self, stream_id: str, status: StreamStatus) -> None:
        with SessionLocal() as session:
            obj = session.get(Stream, stream_id)
            if obj is None:
                return
            obj.status = status.value
            obj.updated_at = datetime.now(timezone.utc)
            session.commit()

    def _build_recorder(self, stream_id: str, stream_url: str, output_dir: str) -> FFmpegRecorder:
        return FFmpegRecorder(
            stream_id=stream_id,
            stream_url=stream_url,
            output_dir=output_dir,
            ffmpeg_path=self.settings.ffmpeg_path,
            segment_seconds=self.settings.segment_time_seconds,
        )

    def _resolve_stream_source(self, req: StartStreamRequest) -> tuple[str, str | None]:
        if req.camera_gb_code:
            dynamic_url = self.camera_stream_service.fetch_temporary_url(req.camera_gb_code)
            return dynamic_url, req.camera_gb_code
        if req.url:
            return req.url, None

        camera_gb_code = self.camera_info_repo.find_camera_gb_code(req.stream_id)
        if not camera_gb_code:
            raise RuntimeError(
                f"stream source missing: provide url/camera_gb_code or configure mapping in camera table for {req.stream_id}"
            )
        dynamic_url = self.camera_stream_service.fetch_temporary_url(camera_gb_code)
        return dynamic_url, camera_gb_code

    async def start_stream(self, req: StartStreamRequest) -> StreamStatusResponse:
        async with self._lock:
            if req.stream_id in self._streams and self._streams[req.stream_id].recorder.is_alive():
                runtime = self._streams[req.stream_id]
                return self._to_status(req.stream_id, runtime)

            if len(self._streams) >= self.settings.max_streams_per_node and req.stream_id not in self._streams:
                raise RuntimeError("max streams reached on this node")

            if self.redis_lock:
                acquired = await self.redis_lock.acquire(req.stream_id)
                if not acquired:
                    raise RuntimeError(f"stream lock exists: {req.stream_id}")

            stream_url, camera_gb_code = self._resolve_stream_source(req)
            output_dir = req.output_dir or os.path.join(self.settings.data_dir, req.stream_id)
            recorder = self._build_recorder(req.stream_id, stream_url, output_dir)
            runtime = self._streams.get(
                req.stream_id,
                StreamRuntime(recorder=recorder, output_dir=output_dir, current_url=stream_url),
            )
            runtime.recorder = recorder
            runtime.output_dir = output_dir
            runtime.current_url = stream_url
            runtime.camera_gb_code = camera_gb_code
            runtime.status = StreamStatus.STARTING
            self._reset_recovery_state(runtime)
            self._streams[req.stream_id] = runtime
            self._upsert_stream_db(req.stream_id, stream_url, output_dir, StreamStatus.STARTING)

            try:
                recorder.start(startup_probe_seconds=self.settings.ffmpeg_startup_probe_seconds)
                runtime.status = StreamStatus.RUNNING
                runtime.last_error = None
                self._reset_recovery_state(runtime)
                self._update_stream_status_db(req.stream_id, StreamStatus.RUNNING)
                logger.info("stream running", extra={"stream_id": req.stream_id, "event": "start", "status": "RUNNING"})
            except Exception as exc:
                runtime.status = StreamStatus.ERROR
                runtime.last_error = str(exc)
                recording_errors_total.labels(stream_id=req.stream_id, reason="start_failed").inc()
                self._update_stream_status_db(req.stream_id, StreamStatus.ERROR)
                if self.redis_lock:
                    await self.redis_lock.release(req.stream_id)
                raise
            finally:
                self._set_metrics()

            return self._to_status(req.stream_id, runtime)

    async def stop_stream(self, stream_id: str) -> StreamStatusResponse:
        async with self._lock:
            runtime = self._streams.get(stream_id)
            if runtime is None:
                return StreamStatusResponse(
                    stream_id=stream_id,
                    status=StreamStatus.STOPPED.value,
                    alive=False,
                    restarts=0,
                    node_id=self.settings.node_id,
                )
            runtime.recorder.stop()
            runtime.status = StreamStatus.STOPPED
            self._reset_recovery_state(runtime)
            self._update_stream_status_db(stream_id, StreamStatus.STOPPED)
            if self.redis_lock:
                await self.redis_lock.release(stream_id)
            self._set_metrics()
            return self._to_status(stream_id, runtime)

    async def handle_stream_failure(self, stream_id: str, reason: str) -> None:
        async with self._lock:
            runtime = self._streams.get(stream_id)
            if runtime is None:
                return
            now = self._utc_now()
            if runtime.status == StreamStatus.STOPPED:
                return
            if runtime.next_retry_at and now < runtime.next_retry_at:
                return
            if runtime.circuit_open_until and now < runtime.circuit_open_until:
                runtime.status = StreamStatus.CIRCUIT_OPEN
                self._update_stream_status_db(stream_id, StreamStatus.CIRCUIT_OPEN)
                return

            runtime.status = StreamStatus.RESTARTING
            runtime.last_error = reason
            self._update_stream_status_db(stream_id, StreamStatus.RESTARTING)
            recording_errors_total.labels(stream_id=stream_id, reason=reason).inc()
            try:
                runtime.recorder.stop()
                if runtime.camera_gb_code:
                    refreshed_url = self.camera_stream_service.fetch_temporary_url(runtime.camera_gb_code)
                    runtime.current_url = refreshed_url
                    runtime.recorder = self._build_recorder(stream_id, refreshed_url, runtime.output_dir)
                    self._upsert_stream_db(stream_id, refreshed_url, runtime.output_dir, StreamStatus.RESTARTING)
                runtime.recorder.start(startup_probe_seconds=self.settings.ffmpeg_startup_probe_seconds)
                runtime.status = StreamStatus.RUNNING
                runtime.restarts += 1
                runtime.last_error = None
                self._reset_recovery_state(runtime)
                self._update_stream_status_db(stream_id, StreamStatus.RUNNING)
                recording_restart_count.labels(stream_id=stream_id).inc()
                logger.warning(
                    "stream restarted",
                    extra={"stream_id": stream_id, "event": "restart", "reason": reason, "status": "RUNNING"},
                )
            except Exception as exc:
                runtime.consecutive_failures += 1
                runtime.last_error = str(exc)
                if runtime.consecutive_failures >= self.settings.restart_max_attempts:
                    cooldown = max(1, self.settings.circuit_breaker_cooldown_seconds)
                    runtime.status = StreamStatus.CIRCUIT_OPEN
                    runtime.circuit_open_until = now + timedelta(seconds=cooldown)
                    runtime.next_retry_at = runtime.circuit_open_until
                    self._update_stream_status_db(stream_id, StreamStatus.CIRCUIT_OPEN)
                    logger.error(
                        "stream circuit open",
                        extra={
                            "stream_id": stream_id,
                            "event": "circuit_open",
                            "reason": runtime.last_error,
                            "status": "CIRCUIT_OPEN",
                        },
                    )
                else:
                    backoff_seconds = self._compute_backoff_seconds(runtime.consecutive_failures)
                    runtime.status = StreamStatus.ERROR
                    runtime.next_retry_at = now + timedelta(seconds=backoff_seconds)
                    self._update_stream_status_db(stream_id, StreamStatus.ERROR)
                    logger.warning(
                        "stream restart failed, will retry",
                        extra={
                            "stream_id": stream_id,
                            "event": "retry_backoff",
                            "reason": f"attempt={runtime.consecutive_failures},backoff={backoff_seconds}s",
                            "status": "ERROR",
                        },
                    )
            finally:
                self._set_metrics()

    async def evaluate_recovery(self, stream_id: str) -> None:
        async with self._lock:
            runtime = self._streams.get(stream_id)
            if runtime is None:
                return
            now = self._utc_now()
            should_retry = False
            reason = "scheduled_retry"

            if runtime.status == StreamStatus.CIRCUIT_OPEN:
                if runtime.circuit_open_until and now >= runtime.circuit_open_until:
                    runtime.circuit_open_until = None
                    runtime.next_retry_at = None
                    should_retry = True
                    reason = "circuit_half_open"
            elif runtime.status == StreamStatus.ERROR:
                if runtime.next_retry_at is None or now >= runtime.next_retry_at:
                    should_retry = True
                    reason = "error_retry"

        if should_retry:
            await self.handle_stream_failure(stream_id, reason)

    def _to_status(self, stream_id: str, runtime: StreamRuntime) -> StreamStatusResponse:
        return StreamStatusResponse(
            stream_id=stream_id,
            status=runtime.status.value,
            alive=runtime.recorder.is_alive(),
            restarts=runtime.restarts,
            node_id=self.settings.node_id,
            last_error=runtime.last_error or runtime.recorder.last_error,
        )

    async def list_status(self) -> list[StreamStatusResponse]:
        async with self._lock:
            return [self._to_status(stream_id, runtime) for stream_id, runtime in self._streams.items()]

    async def get_status(self, stream_id: str) -> StreamStatusResponse:
        async with self._lock:
            runtime = self._streams.get(stream_id)
            if runtime is None:
                return StreamStatusResponse(
                    stream_id=stream_id,
                    status=StreamStatus.STOPPED.value,
                    alive=False,
                    restarts=0,
                    node_id=self.settings.node_id,
                )
            return self._to_status(stream_id, runtime)

    async def snapshot(self) -> dict[str, StreamRuntime]:
        async with self._lock:
            return dict(self._streams)

    async def restore_from_db(self) -> None:
        with SessionLocal() as session:
            rows = session.scalars(select(Stream).where(Stream.node_id == self.settings.node_id)).all()
        for row in rows:
            if row.status not in (StreamStatus.RUNNING.value, StreamStatus.STARTING.value, StreamStatus.RESTARTING.value):
                continue
            req = StartStreamRequest(stream_id=row.id, url=row.url, output_dir=row.output_dir)
            try:
                await self.start_stream(req)
            except Exception as exc:
                logger.error(
                    "failed to restore stream",
                    extra={"stream_id": row.id, "event": "restore", "reason": str(exc), "status": "ERROR"},
                )

    async def stop_all(self) -> None:
        ids = [item.stream_id for item in await self.list_status()]
        for stream_id in ids:
            await self.stop_stream(stream_id)

    async def delete_stream(self, stream_id: str, purge_files: bool = False) -> str:
        output_dir: str | None = None
        runtime_existed = False

        async with self._lock:
            runtime = self._streams.get(stream_id)
            if runtime is not None:
                runtime_existed = True
                output_dir = runtime.recorder.output_dir
                runtime.recorder.stop()
                runtime.status = StreamStatus.STOPPED
                self._streams.pop(stream_id, None)
            if self.redis_lock:
                await self.redis_lock.release(stream_id)
            self._set_metrics()

        stream_row_existed = False
        with SessionLocal() as session:
            stream_row = session.get(Stream, stream_id)
            if stream_row is not None:
                stream_row_existed = True
                output_dir = output_dir or stream_row.output_dir
                session.execute(delete(RecordFile).where(RecordFile.stream_id == stream_id))
                session.delete(stream_row)
                session.commit()
            else:
                # Keep operation idempotent even if stream row is missing.
                deleted_files = session.execute(delete(RecordFile).where(RecordFile.stream_id == stream_id))
                session.commit()
                if deleted_files.rowcount:
                    logger.warning(
                        "deleted orphan record_file rows for missing stream",
                        extra={"stream_id": stream_id, "event": "delete_orphan"},
                    )

        files_deleted = False
        if purge_files:
            target_dir = Path(output_dir or os.path.join(self.settings.data_dir, stream_id))
            if target_dir.exists():
                shutil.rmtree(target_dir)
                files_deleted = True

        removed_any = runtime_existed or stream_row_existed
        message = f"stream_deleted={removed_any}"
        if purge_files:
            message += f", files_deleted={files_deleted}"
        return message
