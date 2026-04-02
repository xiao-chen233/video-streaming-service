from __future__ import annotations

import asyncio
import logging

from app.services.stream_manager import StreamManager, StreamStatus


logger = logging.getLogger(__name__)


class HealthMonitor:
    def __init__(self, stream_manager: StreamManager, interval_seconds: int, no_output_timeout_seconds: int):
        self.stream_manager = stream_manager
        self.interval_seconds = interval_seconds
        self.no_output_timeout_seconds = no_output_timeout_seconds
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="health-monitor")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            await self._task

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            snapshot = await self.stream_manager.snapshot()
            for stream_id, runtime in snapshot.items():
                if runtime.status in (StreamStatus.ERROR, StreamStatus.CIRCUIT_OPEN):
                    await self.stream_manager.evaluate_recovery(stream_id)
                    continue
                if runtime.status in (StreamStatus.RUNNING, StreamStatus.RESTARTING):
                    if not runtime.recorder.is_alive():
                        await self.stream_manager.handle_stream_failure(stream_id, "process_dead")
                        continue
                    if runtime.recorder.has_output_timeout(self.no_output_timeout_seconds):
                        await self.stream_manager.handle_stream_failure(stream_id, "no_output_timeout")
                        continue
            await asyncio.sleep(self.interval_seconds)
        logger.info("health monitor stopped")
