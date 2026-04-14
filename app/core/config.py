from functools import lru_cache
import os

from pydantic import Field, NonNegativeInt
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "recording-service"
    app_host: str = "0.0.0.0"
    app_port: int = 8000

    ffmpeg_path: str = "ffmpeg"
    data_dir: str = "/data/recordings"
    segment_time_seconds: int = 3600
    monitor_interval_seconds: int = 8
    index_interval_seconds: int = 15
    no_output_timeout_seconds: int = 30
    max_streams_per_node: int = 64
    restart_max_attempts: NonNegativeInt = 6
    restart_backoff_base_seconds: NonNegativeInt = 2
    restart_backoff_max_seconds: NonNegativeInt = 60
    circuit_breaker_cooldown_seconds: NonNegativeInt = 120
    ffmpeg_startup_probe_seconds: NonNegativeInt = 2
    camera_index_api_url: str = "http://eom.chnenergy.com.cn/wfjt/video/getIndexCode"
    camera_index_api_protocol: str = "rtmp"
    camera_index_api_timeout_seconds: NonNegativeInt = 6

    db_url: str = "postgresql+psycopg2://recording:recording@localhost:5432/recording"
    redis_url: str | None = None
    redis_lock_ttl_seconds: int = 120

    kafka_enabled: bool = False
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "recording.commands"
    kafka_group_id: str = "recording-service"

    node_id: str = Field(default_factory=lambda: os.getenv("HOSTNAME", "local-node"))
    log_level: str = "INFO"
    log_json: bool = True


@lru_cache
def get_settings() -> Settings:
    return Settings()
