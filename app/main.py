from __future__ import annotations

import signal

from fastapi import FastAPI, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api.routes_cameras import router as camera_router
from app.api.routes_streams import router as stream_router
from app.core.config import get_settings
from app.core.database import init_db
from app.core.logging import setup_logging
from app.services.health_monitor import HealthMonitor
from app.services.segment_indexer import SegmentIndexer
from app.services.stream_manager import StreamManager
from app.ui.routes_dashboard import router as ui_router
from app.workers.kafka_consumer import KafkaCommandConsumer
from app.workers.kafka_producer import KafkaCommandProducer
from app.workers.redis_lock import RedisLockManager


settings = get_settings()
setup_logging(settings.log_level, settings.log_json)

app = FastAPI(title=settings.app_name)
app.include_router(stream_router)
app.include_router(camera_router)
app.include_router(ui_router)


@app.get("/metrics")
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.on_event("startup")
async def on_startup() -> None:
    init_db()
    redis_lock = RedisLockManager(settings.redis_url, settings.redis_lock_ttl_seconds) if settings.redis_url else None
    stream_manager = StreamManager(settings=settings, redis_lock=redis_lock)
    app.state.stream_manager = stream_manager
    app.state.redis_lock = redis_lock

    app.state.health_monitor = HealthMonitor(
        stream_manager=stream_manager,
        interval_seconds=settings.monitor_interval_seconds,
        no_output_timeout_seconds=settings.no_output_timeout_seconds,
    )
    await app.state.health_monitor.start()

    app.state.segment_indexer = SegmentIndexer(settings.data_dir, settings.index_interval_seconds)
    await app.state.segment_indexer.start()

    app.state.kafka_consumer = None
    app.state.kafka_producer = None
    if settings.kafka_enabled:
        app.state.kafka_producer = KafkaCommandProducer(settings.kafka_bootstrap_servers, settings.kafka_topic)
        await app.state.kafka_producer.start()
        app.state.kafka_consumer = KafkaCommandConsumer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic,
            group_id=settings.kafka_group_id,
            manager=stream_manager,
        )
        await app.state.kafka_consumer.start()

    await stream_manager.restore_from_db()
    _install_sigterm_handler()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if getattr(app.state, "kafka_consumer", None):
        await app.state.kafka_consumer.stop()
    if getattr(app.state, "kafka_producer", None):
        await app.state.kafka_producer.stop()
    if getattr(app.state, "segment_indexer", None):
        await app.state.segment_indexer.stop()
    if getattr(app.state, "health_monitor", None):
        await app.state.health_monitor.stop()
    if getattr(app.state, "stream_manager", None):
        await app.state.stream_manager.stop_all()
    if getattr(app.state, "redis_lock", None):
        await app.state.redis_lock.close()


def _install_sigterm_handler() -> None:
    loop = None
    try:
        import asyncio

        loop = asyncio.get_running_loop()
    except Exception:
        return

    def _on_term() -> None:
        loop.create_task(on_shutdown())

    try:
        loop.add_signal_handler(signal.SIGTERM, _on_term)
    except NotImplementedError:
        pass
