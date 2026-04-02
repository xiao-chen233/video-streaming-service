from typing import Literal

from pydantic import BaseModel, Field


class StartStreamRequest(BaseModel):
    stream_id: str = Field(min_length=1, max_length=128)
    url: str = Field(min_length=1)
    output_dir: str | None = None


class StopStreamRequest(BaseModel):
    stream_id: str = Field(min_length=1, max_length=128)


class BatchStartRequest(BaseModel):
    streams: list[StartStreamRequest]


class BatchStopRequest(BaseModel):
    stream_ids: list[str]


class StreamStatusResponse(BaseModel):
    stream_id: str
    status: str
    alive: bool
    restarts: int
    node_id: str
    last_error: str | None = None


class StreamCatalogItem(BaseModel):
    stream_id: str
    url: str
    output_dir: str
    status: str
    node_id: str
    updated_at: str


class StandardResponse(BaseModel):
    ok: bool
    message: str


class CommandMessage(BaseModel):
    action: Literal["start", "stop"]
    stream_id: str
    url: str | None = None
    output: str | None = None
