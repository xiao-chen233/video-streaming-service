from __future__ import annotations

import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from app.api.schemas import CommandMessage, StartStreamRequest
from app.services.stream_manager import StreamManager

logger = logging.getLogger(__name__)


class KafkaCommandConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str, manager: StreamManager):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.manager = manager
        self.consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        await self.consumer.start()
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="kafka-command-consumer")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            await self._task
        if self.consumer:
            await self.consumer.stop()
            self.consumer = None

    async def _run(self) -> None:
        assert self.consumer is not None
        while not self._stop_event.is_set():
            try:
                result = await self.consumer.getmany(timeout_ms=1000, max_records=100)
            except Exception as exc:
                logger.error("kafka consume error: %s", exc)
                await asyncio.sleep(1)
                continue

            for _, messages in result.items():
                for message in messages:
                    await self._handle_message(message.value)

    async def _handle_message(self, payload: dict) -> None:
        try:
            cmd = CommandMessage.model_validate(payload)
        except Exception as exc:
            logger.warning("invalid command payload: %s, err=%s", payload, exc)
            return
        if cmd.action == "start":
            if not cmd.url and not cmd.camera_gb_code:
                logger.warning("start command missing both url and camera_gb_code: %s", payload)
                return
            req = StartStreamRequest(
                stream_id=cmd.stream_id,
                url=cmd.url,
                camera_gb_code=cmd.camera_gb_code,
                output_dir=cmd.output,
            )
            await self.manager.start_stream(req)
        elif cmd.action == "stop":
            await self.manager.stop_stream(cmd.stream_id)
