from __future__ import annotations

import threading
import zlib

from snowflake_id_toolkit import TwitterSnowflakeIDGenerator

from app.core.config import get_settings


SNOWFLAKE_EPOCH_MS = 1704067200000  # 2024-01-01 00:00:00 UTC
SNOWFLAKE_MAX_WORKER_ID = 1023  # Twitter Snowflake: 10 bit node_id


def _worker_id_from_node(node_id: str) -> int:
    return zlib.crc32(node_id.encode("utf-8")) % (SNOWFLAKE_MAX_WORKER_ID + 1)


_generator: TwitterSnowflakeIDGenerator | None = None
_generator_lock = threading.Lock()


def generate_snowflake_id() -> int:
    global _generator
    if _generator is not None:
        return int(_generator.generate_next_id())

    with _generator_lock:
        if _generator is None:
            settings = get_settings()
            worker_id = _worker_id_from_node(settings.node_id)
            _generator = TwitterSnowflakeIDGenerator(node_id=worker_id, epoch=SNOWFLAKE_EPOCH_MS)
    return int(_generator.generate_next_id())
