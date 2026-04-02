# Recording Service

Production-oriented recording service based on persistent FFmpeg workers, stream lifecycle management, self-healing, and Kubernetes deployment patterns.

中文文档: [README.zh-CN.md](./README.zh-CN.md)

## Features

- Persistent FFmpeg process per stream (no cron, no hourly process restart)
- Stream lifecycle state machine
- Auto restart on process crash / no-output timeout
- Retry limit + exponential backoff + circuit breaker (`CIRCUIT_OPEN`)
- Hourly segment output (`segment_atclocktime=1`)
- REST API for start/stop/status/list + batch operations
- Segment indexer writes file metadata into DB
- Optional distributed control via Kafka topic `recording.commands`
- Optional duplicate prevention via Redis lock (`SETNX`)
- Prometheus metrics endpoint `/metrics`
- Graceful shutdown for Kubernetes (`SIGTERM`)

## Project Layout

```text
recording-service/
├── app/
│   ├── api/
│   ├── core/
│   ├── models/
│   ├── services/
│   ├── workers/
│   └── main.py
├── k8s/
├── tests/
├── Dockerfile
└── requirements.txt
```

## Quick Start

```bash
conda env create -f environment.yml
conda activate recording-service
cp .env.example .env
docker compose -f docker-compose.yml up -d
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Local Dependencies (Docker Compose)

`docker-compose.yml` includes:

- PostgreSQL (`localhost:5432`)
- Redis (`localhost:6379`)
- Kafka (`localhost:9094`)
- Kafka UI (`http://localhost:8081`)
- PostgreSQL init SQL (`docker/postgres/init/01-schema.sql`) runs automatically on first startup

Start dependencies:

```bash
docker compose -f docker-compose.yml up -d
```

If you need to re-run PostgreSQL initialization scripts, remove PG volume first:

```bash
docker compose -f docker-compose.yml down -v
docker compose -f docker-compose.yml up -d
```

Stop dependencies:

```bash
docker compose -f docker-compose.yml down
```

## API

- `POST /streams/start`
- `POST /streams/stop`
- `DELETE /streams/{stream_id}?purge_files=true|false`
- `GET /streams/status/{stream_id}`
- `GET /streams/list`
- `GET /streams/catalog`
- `POST /streams/start/batch`
- `POST /streams/stop/batch`

Web Console:

- `GET /` 控制页面（添加流、查看流、启停控制、自动刷新）

Example:

```bash
curl -X POST http://127.0.0.1:8000/streams/start \
  -H "Content-Type: application/json" \
  -d '{"stream_id":"camera_1","url":"rtsp://example/live","output_dir":"/data/recordings/camera_1"}'
```

## Required FFmpeg Arguments

The recorder uses:

```bash
ffmpeg \
 -rtsp_transport tcp \
 -i <stream_url> \
 -c copy \
 -f segment \
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

## Kubernetes

- Single Pod multi-stream: `k8s/deployment.yaml`
- One stream per Pod: `k8s/statefulset-one-stream.yaml`
- Shared config: `k8s/configmap.yaml`
- Persistent storage: `k8s/pvc.yaml`

## Notes

- PostgreSQL is the default database for local and production-style setup.
- Use `REDIS_URL` and `KAFKA_ENABLED=true` to enable distributed control.

## Recovery Strategy

- `RESTART_MAX_ATTEMPTS`: max consecutive restart failures before opening circuit
- `RESTART_BACKOFF_BASE_SECONDS`: exponential backoff base delay
- `RESTART_BACKOFF_MAX_SECONDS`: exponential backoff upper bound
- `CIRCUIT_BREAKER_COOLDOWN_SECONDS`: cooldown duration when `CIRCUIT_OPEN`
- `FFMPEG_STARTUP_PROBE_SECONDS`: startup probe window; quick-exit process counts as failed restart

Status flow on unstable streams:

`RUNNING -> ERROR (backoff retry) -> CIRCUIT_OPEN (cooldown) -> RESTARTING -> RUNNING`
