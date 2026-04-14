# 录像服务（Recording Service）

基于常驻 FFmpeg 进程、流生命周期管理与自恢复机制的生产级录像服务，支持 Kubernetes 与分布式控制。

## 功能特性

- 每路流使用常驻 FFmpeg 进程（不使用 cron，不按小时重启进程）
- 流状态机管理（`INIT/STARTING/RUNNING/ERROR/RESTARTING/CIRCUIT_OPEN/STOPPED`）
- 进程异常与无输出自动恢复
- 重试上限 + 指数退避 + 熔断冷却（`CIRCUIT_OPEN`）
- 按小时切片（`segment_atclocktime=1`，按协议自动选择封装）
- REST API：启动 / 停止 / 删除 / 状态 / 列表 / 批量操作
- 分片索引器自动扫描文件并写入数据库
- Kafka 分布式控制（`recording.commands`）
- Redis 分布式锁防重复拉流（`SETNX`）
- Prometheus 指标（`/metrics`）
- 优雅退出（支持 K8s `SIGTERM`）
- 内置控制台页面（`GET /`）

## 项目结构

```text
recording-service/
├── app/
│   ├── api/
│   ├── core/
│   ├── models/
│   ├── services/
│   ├── ui/
│   ├── workers/
│   └── main.py
├── docker/
├── k8s/
├── tests/
├── docker-compose.yml
├── Dockerfile
└── environment.yml
```

## 快速开始（本地）

```bash
conda env create -f environment.yml
conda activate recording-service
cp .env.example .env
docker compose -f docker-compose.yml up -d
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

访问：

- 控制台页面：`http://127.0.0.1:8000/`
- 健康检查：`http://127.0.0.1:8000/streams/healthz`
- 指标：`http://127.0.0.1:8000/metrics`
- Kafka UI：`http://127.0.0.1:8081`

## Docker Compose 依赖

`docker-compose.yml` 包含：

- PostgreSQL（`localhost:5432`）
- Redis（`localhost:6379`）
- Kafka（`localhost:9094`）
- Kafka UI（`localhost:8081`）

PostgreSQL 首次启动会执行初始化 SQL：

- `docker/postgres/init/01-schema.sql`

常用命令：

```bash
# 启动
docker compose -f docker-compose.yml up -d

# 停止
docker compose -f docker-compose.yml down

# 清空卷并重建（会重新初始化 PG）
docker compose -f docker-compose.yml down -v
docker compose -f docker-compose.yml up -d
```

## API 一览

- `POST /streams/start`
- `POST /streams/stop`
- `DELETE /streams/{stream_id}?purge_files=true|false`
- `GET /streams/status/{stream_id}`
- `GET /streams/list`
- `GET /streams/catalog`
- `POST /streams/start/batch`
- `POST /streams/stop/batch`

### 删除流（停流 + 删元数据 + 可选删文件）

```bash
curl -X DELETE "http://127.0.0.1:8000/streams/camera_1?purge_files=true"
```

说明：

- `purge_files=false`：仅删除内存/数据库元数据，不删录像目录
- `purge_files=true`：额外删除流目录文件
- 接口幂等：重复调用不会报错

## FFmpeg 参数（当前实现）

按输入协议自适应：

- RTSP/RTSPS：强制 `-rtsp_transport tcp`，切片输出 `mp4`
- RTMP/RTMPS：不添加 RTSP 专属参数，切片输出 `flv`

RTSP 示例：

```bash
ffmpeg \
 -rtsp_transport tcp \
 -i <stream_url> \
 -c copy \
 -f segment \
 -segment_format mp4 \
 -segment_time 3600 \
 -segment_atclocktime 1 \
 -strftime 1 \
 -reset_timestamps 1 \
 -movflags +faststart \
 -reconnect 1 \
 -reconnect_streamed 1 \
 -reconnect_delay_max 2 \
 output/%Y%m%d_%H.mp4
```

RTMP 示例：

```bash
ffmpeg \
 -i <stream_url> \
 -c copy \
 -f segment \
 -segment_format flv \
 -segment_time 3600 \
 -segment_atclocktime 1 \
 -strftime 1 \
 -reset_timestamps 1 \
 -reconnect 1 \
 -reconnect_streamed 1 \
 -reconnect_delay_max 2 \
 output/%Y%m%d_%H.flv
```

## 重试 / 退避 / 熔断策略

关键配置项：

- `RESTART_MAX_ATTEMPTS`：连续失败达到阈值后进入熔断
- `RESTART_BACKOFF_BASE_SECONDS`：指数退避起始秒数
- `RESTART_BACKOFF_MAX_SECONDS`：指数退避上限秒数
- `CIRCUIT_BREAKER_COOLDOWN_SECONDS`：熔断冷却时长
- `FFMPEG_STARTUP_PROBE_SECONDS`：启动探针窗口，快速退出计为失败

状态流转：

`RUNNING -> ERROR(退避) -> CIRCUIT_OPEN(冷却) -> RESTARTING -> RUNNING`

## Kubernetes 部署

- 单 Pod 多流：`k8s/deployment.yaml`
- 一流一 Pod：`k8s/statefulset-one-stream.yaml`
- 配置：`k8s/configmap.yaml`
- 存储：`k8s/pvc.yaml`
- 服务：`k8s/service.yaml`

## 说明

- 当前默认数据库为 PostgreSQL。
- 若启用分布式控制，请配置：
  - `REDIS_URL`
  - `KAFKA_ENABLED=true`
  - `KAFKA_BOOTSTRAP_SERVERS`
