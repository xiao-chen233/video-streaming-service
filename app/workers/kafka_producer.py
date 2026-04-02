from __future__ import annotations

import json

from aiokafka import AIOKafkaProducer

from app.api.schemas import CommandMessage


class KafkaCommandProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        if self.producer:
            return
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x, ensure_ascii=True).encode("utf-8"),
        )
        await self.producer.start()

    async def stop(self) -> None:
        if self.producer:
            await self.producer.stop()
            self.producer = None

    async def publish(self, message: CommandMessage) -> None:
        if not self.producer:
            raise RuntimeError("kafka producer not started")
        await self.producer.send_and_wait(self.topic, message.model_dump())
