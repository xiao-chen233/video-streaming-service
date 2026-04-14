# AGENTS.md

## 项目目标

本项目是生产级录像服务，核心能力包括：

- 常驻 FFmpeg 进程录制
- 多路流生命周期管理
- 自动恢复（重试上限 + 指数退避 + 熔断）
- 分片文件索引入库
- 分布式控制（Kafka / Redis）
- Docker Compose 与 Kubernetes 部署

## 开发环境

### Python 环境（Conda）

```bash
conda env create -f environment.yml
conda activate recording-service
```

### 本地依赖

```bash
docker compose -f docker-compose.yml up -d
```

### 启动服务

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 关键目录

- `app/main.py`：应用入口与生命周期管理
- `app/api/`：REST API
- `app/services/`：核心业务（流管理、监控、索引）
- `app/workers/`：Kafka/Redis 相关组件
- `app/ui/`：控制台页面
- `k8s/`：Kubernetes 清单
- `docker-compose.yml`：本地依赖编排

## 代码约定

- 优先保持接口幂等性（尤其是 start/stop/delete）
- 避免破坏状态机语义（`RUNNING/ERROR/CIRCUIT_OPEN/...`）
- 任何自动恢复策略变更需同步配置项与文档
- 新增配置请同步到：
  - `.env.example`
  - `k8s/configmap.yaml`（如适用）
  - `README.md` / `README.zh-CN.md`

## 验证建议

- 语法检查：

```bash
PYTHONPYCACHEPREFIX=.pycache python3 -m compileall app
```

- 健康检查：

```bash
curl http://127.0.0.1:8000/streams/healthz
```

- 查看流：

```bash
curl http://127.0.0.1:8000/streams/list
curl http://127.0.0.1:8000/streams/catalog
```

## 常用接口

- `POST /streams/start`
- `POST /streams/stop`
- `DELETE /streams/{stream_id}?purge_files=true|false`
- `GET /streams/status/{stream_id}`
- `GET /streams/list`
- `GET /streams/catalog`

## 提交流程

1. 保持改动最小且聚焦目标
2. 更新必要文档
3. 本地完成语法与基础接口验证后再提交
4. 提交信息建议使用 `feat/fix/chore/docs` 前缀
