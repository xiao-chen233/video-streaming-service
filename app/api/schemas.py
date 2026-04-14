from typing import Literal

from pydantic import BaseModel, Field, model_validator


class StartStreamRequest(BaseModel):
    stream_id: str = Field(min_length=1, max_length=128)
    url: str | None = None
    camera_gb_code: str | None = Field(default=None, min_length=1, max_length=64)
    output_dir: str | None = None

    @model_validator(mode="after")
    def _validate_source(self) -> "StartStreamRequest":
        if self.url is not None and not self.url.strip():
            raise ValueError("url cannot be blank")
        return self


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


class CameraInfoItem(BaseModel):
    id: int
    subsidiary_company: str | None = None
    subsidiary_company_id: int | None = None
    subsidiary_company_code: str | None = None
    grassroots_enterprises: str | None = None
    grassroots_enterprises_code: str | None = None
    camera_name: str | None = None
    camera_gb_code: str | None = None
    discharge_port_id: int | None = None
    discharge_port_name: str | None = None
    discharge_port_number: str | None = None
    monitoring_type: str | None = None
    grassroots_enterprises_id: int | None = None


class StandardResponse(BaseModel):
    ok: bool
    message: str


class CommandMessage(BaseModel):
    action: Literal["start", "stop"]
    stream_id: str
    url: str | None = None
    camera_gb_code: str | None = None
    output: str | None = None
