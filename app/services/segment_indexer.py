from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import logging
from pathlib import Path
import time

from sqlalchemy import select

from app.core.database import SessionLocal
from app.core.metrics import recording_file_write_latency
from app.core.snowflake import generate_snowflake_id
from app.models.record_file import RecordFile
from app.models.stream import Stream


logger = logging.getLogger(__name__)


class SegmentIndexer:
    def __init__(self, data_dir: str, interval_seconds: int):
        self.data_dir = Path(data_dir)
        self.interval_seconds = interval_seconds
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._unknown_stream_logged: set[str] = set()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="segment-indexer")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            await self._task

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            start = time.perf_counter()
            try:
                self._index_once()
            except Exception as exc:
                logger.error("indexer error: %s", exc)
            finally:
                recording_file_write_latency.observe(time.perf_counter() - start)
            await asyncio.sleep(self.interval_seconds)

    def _index_once(self) -> None:
        if not self.data_dir.exists():
            return

        with SessionLocal() as session:
            known_stream_ids = set(session.scalars(select(Stream.id)).all())

        for file_path in self.data_dir.rglob("*.mp4"):
            stream_id = file_path.parent.name
            if stream_id not in known_stream_ids:
                key = f"{stream_id}:{str(file_path)}"
                if key not in self._unknown_stream_logged:
                    logger.warning(
                        "skip indexing file without stream metadata: stream_id=%s file=%s",
                        stream_id,
                        str(file_path),
                    )
                    self._unknown_stream_logged.add(key)
                continue
            parsed = self._parse_time(file_path.name)
            if parsed is None:
                continue
            start_time, end_time = parsed
            file_size = file_path.stat().st_size

            with SessionLocal() as session:
                exists = session.scalar(
                    select(RecordFile.id).where(RecordFile.stream_id == stream_id, RecordFile.file_path == str(file_path))
                )
                if exists:
                    continue
                row = RecordFile(
                    id=generate_snowflake_id(),
                    stream_id=stream_id,
                    file_path=str(file_path),
                    start_time=start_time,
                    end_time=end_time,
                    size=file_size,
                )
                session.add(row)
                session.commit()

    @staticmethod
    def _parse_time(filename: str) -> tuple[datetime, datetime] | None:
        base = filename.replace(".mp4", "")
        try:
            dt = datetime.strptime(base, "%Y%m%d_%H").replace(tzinfo=timezone.utc)
        except ValueError:
            return None
        return dt, dt + timedelta(hours=1)
