from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes_streams import router as stream_router
from app.api.schemas import StartStreamRequest, StreamStatusResponse
from app.core.config import Settings
from app.core.snowflake import generate_snowflake_id
from app.services.segment_indexer import SegmentIndexer
from app.services.stream_manager import StreamManager, StreamStatus


# 轻量假实现：用于路由回归测试，避免依赖 FFmpeg/DB/Redis/Kafka 等外部组件。
class FakeStreamManager:
    def __init__(self) -> None:
        self._items: dict[str, StreamStatusResponse] = {}
        self._fail_start = False

    def set_fail_start(self, fail: bool) -> None:
        self._fail_start = fail

    async def start_stream(self, req: StartStreamRequest) -> StreamStatusResponse:
        if self._fail_start:
            raise RuntimeError("start failed")
        item = StreamStatusResponse(
            stream_id=req.stream_id,
            status=StreamStatus.RUNNING.value,
            alive=True,
            restarts=0,
            node_id="test-node",
            last_error=None,
        )
        self._items[req.stream_id] = item
        return item

    async def stop_stream(self, stream_id: str) -> StreamStatusResponse:
        item = self._items.get(stream_id)
        if item is None:
            return StreamStatusResponse(
                stream_id=stream_id,
                status=StreamStatus.STOPPED.value,
                alive=False,
                restarts=0,
                node_id="test-node",
                last_error=None,
            )
        stopped = StreamStatusResponse(
            stream_id=stream_id,
            status=StreamStatus.STOPPED.value,
            alive=False,
            restarts=item.restarts,
            node_id=item.node_id,
            last_error=item.last_error,
        )
        self._items[stream_id] = stopped
        return stopped

    async def delete_stream(self, stream_id: str, purge_files: bool = False) -> str:
        existed = stream_id in self._items
        self._items.pop(stream_id, None)
        return f"stream_deleted={existed}, files_deleted={purge_files}"

    async def get_status(self, stream_id: str) -> StreamStatusResponse:
        return self._items.get(
            stream_id,
            StreamStatusResponse(
                stream_id=stream_id,
                status=StreamStatus.STOPPED.value,
                alive=False,
                restarts=0,
                node_id="test-node",
                last_error=None,
            ),
        )

    async def list_status(self) -> list[StreamStatusResponse]:
        return list(self._items.values())


def _build_test_client(manager: FakeStreamManager) -> TestClient:
    # 仅挂载 streams 路由，聚焦接口行为与错误映射，不触发应用启动生命周期。
    app = FastAPI()
    app.include_router(stream_router)
    app.state.stream_manager = manager
    app.state.kafka_producer = None
    return TestClient(app)


def test_snowflake_id_unique_and_ordered() -> None:
    # 回归目标：雪花 ID 在单进程连续生成时应保持“唯一 + 单调递增”。
    ids = [generate_snowflake_id() for _ in range(200)]
    assert all(isinstance(v, int) for v in ids)
    assert len(set(ids)) == len(ids)
    assert ids == sorted(ids)


def test_segment_indexer_parse_time() -> None:
    # 回归目标：文件名时间解析正确，且非法文件名不会误入库。
    parsed = SegmentIndexer._parse_time("20260101_15.mp4")
    assert parsed is not None
    start_time, end_time = parsed
    assert start_time == datetime(2026, 1, 1, 15, 0, tzinfo=timezone.utc)
    assert end_time - start_time == timedelta(hours=1)
    assert SegmentIndexer._parse_time("bad_name.mp4") is None


def test_stream_routes_start_stop_delete_and_list() -> None:
    # 覆盖单流主流程：start -> status -> list -> stop -> delete。
    manager = FakeStreamManager()
    client = _build_test_client(manager)

    start_resp = client.post(
        "/streams/start",
        json={"stream_id": "cam-1", "url": "rtsp://example/1", "output_dir": "/tmp/cam-1"},
    )
    assert start_resp.status_code == 200
    assert start_resp.json()["status"] == StreamStatus.RUNNING.value

    status_resp = client.get("/streams/status/cam-1")
    assert status_resp.status_code == 200
    assert status_resp.json()["alive"] is True

    list_resp = client.get("/streams/list")
    assert list_resp.status_code == 200
    assert len(list_resp.json()) == 1

    stop_resp = client.post("/streams/stop", json={"stream_id": "cam-1"})
    assert stop_resp.status_code == 200
    assert stop_resp.json()["status"] == StreamStatus.STOPPED.value

    delete_resp = client.delete("/streams/cam-1?purge_files=true")
    assert delete_resp.status_code == 200
    assert delete_resp.json()["ok"] is True
    assert "files_deleted=True" in delete_resp.json()["message"]


def test_stream_routes_batch_start_stop() -> None:
    # 覆盖批量接口，确保批量开始/停止的状态聚合正确。
    manager = FakeStreamManager()
    client = _build_test_client(manager)

    start_batch_resp = client.post(
        "/streams/start/batch",
        json={
            "streams": [
                {"stream_id": "cam-1", "url": "rtsp://example/1", "output_dir": "/tmp/cam-1"},
                {"stream_id": "cam-2", "url": "rtsp://example/2", "output_dir": "/tmp/cam-2"},
            ]
        },
    )
    assert start_batch_resp.status_code == 200
    assert {item["stream_id"] for item in start_batch_resp.json()} == {"cam-1", "cam-2"}

    stop_batch_resp = client.post("/streams/stop/batch", json={"stream_ids": ["cam-1", "cam-2"]})
    assert stop_batch_resp.status_code == 200
    assert all(item["status"] == StreamStatus.STOPPED.value for item in stop_batch_resp.json())


def test_stream_routes_error_mapping_to_http_400() -> None:
    # 回归目标：业务异常应统一映射为 HTTP 400，并透传 detail 便于排障。
    manager = FakeStreamManager()
    manager.set_fail_start(True)
    client = _build_test_client(manager)

    resp = client.post("/streams/start", json={"stream_id": "cam-err", "url": "rtsp://example/err"})
    assert resp.status_code == 400
    assert resp.json()["detail"] == "start failed"


def test_stream_routes_start_support_camera_gb_code_without_url() -> None:
    manager = FakeStreamManager()
    client = _build_test_client(manager)

    start_resp = client.post(
        "/streams/start",
        json={"stream_id": "cam-3", "camera_gb_code": "64018117651329340011", "output_dir": "/tmp/cam-3"},
    )
    assert start_resp.status_code == 200
    assert start_resp.json()["status"] == StreamStatus.RUNNING.value


class _FakeCameraInfoRepo:
    def __init__(self, mapping: dict[str, str]):
        self.mapping = mapping

    def find_camera_gb_code(self, stream_id: str) -> str | None:
        return self.mapping.get(stream_id)


class _FakeCameraStreamService:
    def fetch_temporary_url(self, camera_gb_code: str) -> str:
        return f"rtsp://temporary/{camera_gb_code}"


def test_stream_manager_resolve_stream_source_from_camera_table() -> None:
    settings = Settings()
    manager = StreamManager(
        settings=settings,
        redis_lock=None,
        camera_info_repo=_FakeCameraInfoRepo({"1001": "64018117651329340011"}),
        camera_stream_service=_FakeCameraStreamService(),
    )
    url, gb_code = manager._resolve_stream_source(StartStreamRequest(stream_id="1001"))
    assert gb_code == "64018117651329340011"
    assert url == "rtsp://temporary/64018117651329340011"


def test_stream_manager_resolve_stream_source_uses_direct_url_first() -> None:
    settings = Settings()
    manager = StreamManager(
        settings=settings,
        redis_lock=None,
        camera_info_repo=_FakeCameraInfoRepo({"1001": "64018117651329340011"}),
        camera_stream_service=_FakeCameraStreamService(),
    )
    url, gb_code = manager._resolve_stream_source(StartStreamRequest(stream_id="1001", url="rtsp://manual/url"))
    assert gb_code is None
    assert url == "rtsp://manual/url"
