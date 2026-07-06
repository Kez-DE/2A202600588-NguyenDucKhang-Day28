# Lab28 Submission Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix every broken integration point in this Lab #28 submission, hardsen the API gateway, populate observability, write the required 5-question answers, then verify the whole local stack for real with Docker and capture the required screenshots automatically.

**Architecture:** No restructuring — same flat repo, same Docker Compose topology, same service names/ports. Every change is either a bug fix (things that would crash or silently no-op) or a hardening addition (resilience, provisioning, docs) needed to score well against `SUBMISSION.md`'s rubric.

**Tech Stack:** Docker Compose, Kafka, Prefect 2.14.0, Qdrant, Redis, Prometheus, Grafana, FastAPI, pytest, Playwright (for screenshot capture only).

## Global Constraints

- Repo stays flat at root — no nested `lab28/` subfolder (per user decision).
- No local stub/mock committed for the Kaggle vLLM/embedding services — those are the student's responsibility to run for real before final submission.
- Keep existing env var names exactly: host-side `VLLM_NGROK_URL`, `EMBED_NGROK_URL`, `LANGCHAIN_API_KEY`, `LANGCHAIN_PROJECT`; container-side `VLLM_URL`, `QDRANT_URL`, `REDIS_URL`.
- Keep existing ports/URLs referenced throughout README/SUBMISSION.md unchanged: Prefect `4200`, Grafana `3000`, Qdrant `6333`, Prometheus `9090`, API Gateway `8000`.
- Prefect stays pinned to `2.14.0` — confirmed via `docker run prefecthq/prefect:2.14.0-python3.10` that a decorated flow instance's `.serve(name=..., cron=...)` works on this exact version, so no version bump needed.
- `pandas==2.1.0` / `pyarrow==14.0.0` require `numpy<2` pinned alongside them — confirmed by reproducing `AttributeError: _ARRAY_API not found` inside both a plain Python 3.11 image and the actual `prefecthq/prefect:2.14.0-python3.10` image when installing without the pin.
- Verify every runtime change against real Docker Compose in this environment. The only things this environment cannot verify are the real Kaggle vLLM/embedding calls — call this out explicitly wherever it applies instead of guessing at behavior.

---

### Task 1: Repo housekeeping fixes

**Files:**
- Modify: `.gitignore`
- Create: `.env.example`
- Modify: `README.md`
- Create: `requirements.txt` (repo root)

**Interfaces:**
- Produces: `requirements.txt` at repo root, installable standalone, used by later tasks/scripts run from the host (`scripts/01_ingest_to_kafka.py`, `scripts/03_delta_to_feast.py`, `smoke-tests/test_e2e.py`, `scripts/production_readiness_check.py`).

- [ ] **Step 1: Remove the `.gitignore` bug**

Open `.gitignore` and delete its last line, which currently reads exactly:
```
ANSWERS.md
```
(It sits right after the `.streamlit/secrets.toml` line with no blank line or comment above it — that's the whole bug: any `ANSWERS.md` a student creates is silently excluded from git.)

- [ ] **Step 2: Verify the fix**

Run: `touch ANSWERS.md && git check-ignore -v ANSWERS.md ; echo "exit=$?"; rm ANSWERS.md`
Expected: no output from `git check-ignore`, and `exit=1` (meaning "not ignored"). Before the fix this would have printed `.gitignore:<N>:ANSWERS.md	ANSWERS.md` and `exit=0`.

- [ ] **Step 3: Create `.env.example`**

```bash
# .env.example
# Copy to .env and fill in real values before running docker compose up.

# From your Kaggle notebook's ngrok/cloudflared tunnel (vLLM server, Cell 4 in LAB28_GUIDE.md)
VLLM_NGROK_URL=https://your-vllm-tunnel-url.ngrok-free.app

# From your Kaggle notebook's ngrok/cloudflared tunnel (embedding server, Cell 5)
EMBED_NGROK_URL=https://your-embed-tunnel-url.ngrok-free.app

# From smith.langchain.com -> Settings -> API Keys
LANGCHAIN_API_KEY=your_langsmith_key
LANGCHAIN_PROJECT=lab28-platform
```

- [ ] **Step 4: Fix the `cd lab28` bug in `README.md`**

In the "### 1. Khởi động Local Stack" section, the code block currently is:
```bash
cd lab28
docker compose up -d
docker compose ps  # Kiểm tra tất cả services Up
```
Remove the `cd lab28` line (this repo's `docker-compose.yml` is already at the repo root, not in a `lab28/` subfolder). Result:
```bash
docker compose up -d
docker compose ps  # Kiểm tra tất cả services Up
```

- [ ] **Step 5: Create root `requirements.txt`**

```
# requirements.txt — for running scripts/ and smoke-tests/ from the host.
# Recommended Python: 3.10–3.12 (some pins below don't yet have prebuilt
# wheels for very new Python releases like 3.13/3.14).
numpy<2
pytest==8.2.0
requests==2.32.3
kafka-python==2.0.2
redis==5.0.8
pandas==2.1.0
pyarrow==14.0.0
qdrant-client==1.9.0
langsmith==0.1.99
```

- [ ] **Step 6: Verify it installs cleanly**

Run: `docker run --rm -v "$(pwd)/requirements.txt:/tmp/requirements.txt" python:3.11-slim bash -c "pip install -q -r /tmp/requirements.txt && python -c \"import pytest, requests, kafka, redis, pandas, pyarrow, qdrant_client, langsmith; print('ALL_OK')\""`
Expected: last line of output is `ALL_OK`.

- [ ] **Step 7: Commit**

```bash
git add .gitignore .env.example README.md requirements.txt
git commit -m "Fix ANSWERS.md gitignore bug, add .env.example and root requirements.txt, fix README cd-lab28 bug"
```

---

### Task 2: Harden the API Gateway

**Files:**
- Modify: `api-gateway/main.py`
- Modify: `api-gateway/requirements.txt`
- Modify: `api-gateway/Dockerfile`
- Create: `api-gateway/test_main.py`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `main.py`'s `app` (FastAPI instance) now returns `{"answer": str, "latency_ms": float, "model": str, "degraded": bool}` from `POST /api/v1/chat`, and returns HTTP 422 (not 500) when `query` is missing from the body. `VLLM_URL` now defaults to `""` instead of raising `KeyError` at import time when unset.

- [ ] **Step 1: Write the test file against current (unfixed) `main.py`**

Create `api-gateway/test_main.py`:
```python
from unittest.mock import AsyncMock, patch
import httpx
from fastapi.testclient import TestClient

import main

client = TestClient(main.app)


def test_missing_query_returns_422():
    resp = client.post("/api/v1/chat", json={})
    assert resp.status_code == 422


def test_chat_degrades_when_llm_unreachable():
    with patch("httpx.AsyncClient.post", new=AsyncMock()) as mocked:
        mocked.side_effect = [
            httpx.Response(200, json={"result": []}, request=httpx.Request("POST", "http://qdrant")),
            httpx.ConnectError("boom", request=httpx.Request("POST", "http://vllm")),
        ]
        resp = client.post("/api/v1/chat", json={"query": "hi", "embedding": [0.1] * 384})

    assert resp.status_code == 200
    data = resp.json()
    assert data["degraded"] is True
    assert "answer" in data


def test_chat_degrades_when_qdrant_unreachable_but_llm_ok():
    with patch("httpx.AsyncClient.post", new=AsyncMock()) as mocked:
        mocked.side_effect = [
            httpx.ConnectError("boom", request=httpx.Request("POST", "http://qdrant")),
            httpx.Response(
                200,
                json={
                    "choices": [{"message": {"content": "hello world"}}],
                    "model": "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
                },
                request=httpx.Request("POST", "http://vllm"),
            ),
        ]
        resp = client.post("/api/v1/chat", json={"query": "hi", "embedding": [0.1] * 384})

    assert resp.status_code == 200
    data = resp.json()
    assert data["degraded"] is False
    assert data["answer"] == "hello world"
```

- [ ] **Step 2: Run tests to verify they fail against current code**

Run:
```bash
docker run --rm -v "$(pwd)/api-gateway:/app" -w /app -e VLLM_URL=http://placeholder python:3.10-slim bash -c \
  "pip install -q fastapi==0.104.1 uvicorn==0.24.0 httpx==0.25.0 prometheus-fastapi-instrumentator==7.0.0 pytest==8.2.0 && pytest test_main.py -v"
```
Expected: `test_missing_query_returns_422` FAILS (current code does `body["query"]` on a raw dict, raising an unhandled `KeyError` → 500, not 422), and the two degrade tests FAIL (current code has no try/except, so a mocked `httpx.ConnectError` propagates and the endpoint raises instead of returning 200).

- [ ] **Step 3: Rewrite `main.py`**

```python
# api-gateway/main.py
from fastapi import FastAPI
from pydantic import BaseModel, Field
from prometheus_fastapi_instrumentator import Instrumentator
import httpx, os, time

app = FastAPI(title="AI Platform API Gateway")
Instrumentator().instrument(app).expose(app)  # Integration 9: Prometheus

VLLM_URL = os.environ.get("VLLM_URL", "")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333")


class ChatRequest(BaseModel):
    query: str
    embedding: list[float] = Field(default_factory=lambda: [0.0] * 384)


async def search_context(embedding: list[float]) -> list:
    """Vector search in Qdrant. Degrades to empty context on any failure."""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(
                f"{QDRANT_URL}/collections/documents/points/search",
                json={"vector": embedding, "limit": 3},
            )
            resp.raise_for_status()
            return resp.json().get("result", [])
    except (httpx.HTTPError, httpx.TimeoutException):
        return []


async def call_llm(prompt: str) -> tuple[str, str, bool]:
    """Call vLLM. Returns (answer, model, degraded)."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{VLLM_URL}/v1/chat/completions",
                json={
                    "model": "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            resp.raise_for_status()
            result = resp.json()
            return result["choices"][0]["message"]["content"], result["model"], False
    except (httpx.HTTPError, httpx.TimeoutException, KeyError):
        return (
            "Service temporarily degraded: LLM backend is unavailable. Please retry shortly.",
            "unavailable",
            True,
        )


@app.post("/api/v1/chat")
async def chat(body: ChatRequest):
    start = time.time()
    context = await search_context(body.embedding)
    prompt = f"Context: {context}\n\nQuery: {body.query}"
    answer, model, degraded = await call_llm(prompt)
    latency = (time.time() - start) * 1000

    return {
        "answer": answer,
        "latency_ms": round(latency, 2),
        "model": model,
        "degraded": degraded,
    }


@app.get("/health")
def health():
    return {"status": "ok"}
```

- [ ] **Step 4: Bump `api-gateway/requirements.txt`**

```
fastapi==0.110.0
uvicorn==0.29.0
httpx==0.27.0
prometheus-fastapi-instrumentator==7.0.0
```
(The previous pins — `fastapi==0.104.1` with `prometheus-fastapi-instrumentator==7.0.0` — are mutually unsatisfiable: fastapi 0.104.1 requires `starlette<0.28`, but prometheus-fastapi-instrumentator 7.0.0 requires `starlette>=0.30`. Confirmed via `pip install` producing a `ResolutionImpossible` error.)

- [ ] **Step 5: Fix `api-gateway/Dockerfile` to install from `requirements.txt` instead of an unpinned inline list**

```dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 6: Run tests to verify they pass**

Run:
```bash
docker run --rm -v "$(pwd)/api-gateway:/app" -w /app python:3.10-slim bash -c \
  "pip install -q -r requirements.txt pytest==8.2.0 && pytest test_main.py -v"
```
Expected: `3 passed`.

- [ ] **Step 7: Commit**

```bash
git add api-gateway/main.py api-gateway/requirements.txt api-gateway/Dockerfile api-gateway/test_main.py
git commit -m "Harden API Gateway: graceful degradation, Pydantic validation, fix dependency conflict"
```

---

### Task 3: Fix the Prometheus scrape config

**Files:**
- Modify: `monitoring/prometheus.yml`

- [ ] **Step 1: Remove the two scrape jobs that never expose Prometheus metrics**

```yaml
# monitoring/prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "api-gateway"
    static_configs:
      - targets: ["api-gateway:8000"]
```
(Dropped the `kafka` and `prefect-orion` jobs — Kafka's broker port and Prefect's API port are not Prometheus-format `/metrics` endpoints, so those jobs would sit permanently `up=0` and drag down the Observability score for no real signal.)

- [ ] **Step 2: Verify the config is syntactically valid**

Run: `docker run --rm -v "$(pwd)/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml" prom/prometheus:latest promtool check config /etc/prometheus/prometheus.yml`
Expected: output ends with `SUCCESS` and shows exactly one scrape job (`api-gateway`).

- [ ] **Step 3: Commit**

```bash
git add monitoring/prometheus.yml
git commit -m "Remove Prometheus scrape targets that don't expose metrics endpoints"
```

---

### Task 4: Fix the Prefect flow (schedule bug, deploy API mismatch, numpy pin)

**Files:**
- Modify: `prefect/flows/kafka_to_delta.py`
- Modify: `prefect/flows/requirements.txt`
- Modify: `docker-compose.yml` (rename `prefect-worker` service to `prefect-runner`, simplify its command)
- Modify: `README.md` (update the one `prefect-worker` reference in Troubleshooting)

**Interfaces:**
- Produces: a Prefect deployment named `kafka-to-delta` (flow `Kafka to Delta Pipeline`) registered against `PREFECT_API_URL`, running on a 5-minute cron schedule inside the `prefect-runner` container.

- [ ] **Step 1: Fix `prefect/flows/kafka_to_delta.py`**

```python
# prefect/flows/kafka_to_delta.py
from prefect import flow, task
from kafka import KafkaConsumer
import json, os
import pandas as pd
from datetime import datetime

@task
def consume_and_process():
    """Consume data from Kafka topic"""
    consumer = KafkaConsumer(
        "data.raw",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode())
    )
    records = []
    for msg in consumer:
        records.append(msg.value)

    print(f"Consumed {len(records)} records from Kafka")
    return records

@task
def save_to_delta(records):
    """Save records to Delta Lake (parquet format)"""
    if not records:
        print("No records to save")
        return

    df = pd.DataFrame(records)
    path = "/opt/delta-lake/raw"
    os.makedirs(path, exist_ok=True)
    df.to_parquet(f"{path}/batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
    print(f"Saved {len(df)} records to Delta Lake")

@flow(name="Kafka to Delta Pipeline")
def kafka_to_delta_flow():
    """Main flow: consume from Kafka and save to Delta Lake"""
    records = consume_and_process()
    save_to_delta(records)

if __name__ == "__main__":
    # .serve() registers this as deployment "kafka-to-delta" against
    # PREFECT_API_URL and blocks forever, polling for scheduled/manual runs
    # every 5 minutes. No separate worker/work-pool needed.
    kafka_to_delta_flow.serve(name="kafka-to-delta", cron="*/5 * * * *")
```
Two bugs fixed: `@flow(schedule="* */5 * * *")` is not a valid kwarg on the `@flow` decorator (would raise `TypeError` on import); and even if it were, `"* */5 * * *"` doesn't mean "every 5 minutes" — it means "every minute, during hours 0,5,10,...". Replaced with `.serve(cron="*/5 * * * *")`, which is the correct cron and the correct Prefect 2.14 API (confirmed via `inspect.signature` against the pinned image — `.serve()` exists and accepts `name`/`cron`).

- [ ] **Step 2: Pin `numpy<2` in `prefect/flows/requirements.txt`**

```
prefect==2.14.0
kafka-python==2.0.2
numpy<2
pandas==2.1.0
pyarrow==14.0.0
```
(Without this pin, `pip install pandas==2.1.0 pyarrow==14.0.0` resolves numpy to the 2.x line, and importing pandas then crashes with `AttributeError: _ARRAY_API not found` / `ValueError: numpy.dtype size changed` — reproduced directly inside `prefecthq/prefect:2.14.0-python3.10`.)

- [ ] **Step 3: Simplify the `docker-compose.yml` Prefect worker service**

Replace the existing `prefect-worker` service block:
```yaml
  prefect-worker:
    image: prefecthq/prefect:2.14.0-python3.10
    command: >
      sh -c "prefect work-pool create --type process lab28-worker 2>/dev/null || true
      && prefect worker start -p lab28-worker -n lab28-worker"
    environment:
      PREFECT_API_URL: http://prefect-orion:4200/api
    volumes:
      - ./prefect/flows:/opt/prefect/flows
      - ./delta-lake:/opt/delta-lake
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on: [prefect-orion, kafka]
```
with:
```yaml
  prefect-runner:
    image: prefecthq/prefect:2.14.0-python3.10
    command: >
      sh -c "pip install -q -r /opt/prefect/flows/requirements.txt &&
      python /opt/prefect/flows/kafka_to_delta.py"
    environment:
      PREFECT_API_URL: http://prefect-orion:4200/api
    volumes:
      - ./prefect/flows:/opt/prefect/flows
      - ./delta-lake:/opt/delta-lake
    depends_on: [prefect-orion, kafka]
```
(Renamed because it's no longer a generic Prefect worker polling a work-pool — `.serve()` runs the flow's own scheduling loop in-process, so the separate work-pool/worker machinery and the `docker.sock` mount it needed are gone.)

- [ ] **Step 4: Update the one README reference to the old service name**

In `README.md`'s Troubleshooting section, change:
```
docker compose logs prefect-worker
```
to:
```
docker compose logs prefect-runner
```

- [ ] **Step 5: Verify locally with real Docker**

Run:
```bash
docker compose up -d zookeeper kafka prefect-orion prefect-runner
sleep 25
docker compose ps prefect-runner
docker compose logs prefect-runner | grep -i "being served"
```
Expected: `docker compose ps prefect-runner` shows state `Up` (not `Restarting`), and the logs contain a line like `Your flow 'Kafka to Delta Pipeline' is being served and polling for scheduled runs!`.

- [ ] **Step 6: Commit**

```bash
git add prefect/flows/kafka_to_delta.py prefect/flows/requirements.txt docker-compose.yml README.md
git commit -m "Fix Prefect flow: invalid schedule kwarg, deploy API mismatch, wrong cron, numpy pin"
```

---

### Task 5: Grafana provisioning (datasource + dashboard)

**Files:**
- Create: `monitoring/grafana/provisioning/datasources/prometheus.yml`
- Create: `monitoring/grafana/provisioning/dashboards/dashboards.yml`
- Create: `monitoring/grafana/dashboards/api-gateway.json`
- Modify: `docker-compose.yml` (grafana service volumes)

**Interfaces:**
- Consumes: the `api-gateway` Prometheus job from Task 3, and the real metric names confirmed below.
- Produces: a Grafana dashboard titled "Lab28 API Gateway" (uid `lab28-api-gateway`), pre-loaded on container start.

Real metric names were confirmed by instrumenting a throwaway FastAPI app with `prometheus-fastapi-instrumentator==7.0.0` and curling `/metrics`: `http_requests_total{handler,method,status}` (status is a bucket like `"2xx"`/`"4xx"`/`"5xx"`, not an exact code) and `http_request_duration_seconds_bucket{handler,method,le}`.

- [ ] **Step 1: Create the datasource provisioning file**

```yaml
# monitoring/grafana/provisioning/datasources/prometheus.yml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

- [ ] **Step 2: Create the dashboard provider file**

```yaml
# monitoring/grafana/provisioning/dashboards/dashboards.yml
apiVersion: 1

providers:
  - name: "Lab28 dashboards"
    orgId: 1
    folder: ""
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    options:
      path: /var/lib/grafana/dashboards
```

- [ ] **Step 3: Create the dashboard JSON**

```json
{
  "uid": "lab28-api-gateway",
  "title": "Lab28 API Gateway",
  "timezone": "browser",
  "schemaVersion": 39,
  "version": 1,
  "refresh": "10s",
  "time": { "from": "now-15m", "to": "now" },
  "panels": [
    {
      "id": 1,
      "title": "Request Rate",
      "type": "timeseries",
      "datasource": "Prometheus",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 },
      "targets": [
        { "expr": "sum(rate(http_requests_total[1m]))", "legendFormat": "requests/sec", "refId": "A" }
      ]
    },
    {
      "id": 2,
      "title": "P95 Latency",
      "type": "timeseries",
      "datasource": "Prometheus",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 },
      "fieldConfig": { "defaults": { "unit": "s" }, "overrides": [] },
      "targets": [
        {
          "expr": "histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le))",
          "legendFormat": "p95",
          "refId": "A"
        }
      ]
    },
    {
      "id": 3,
      "title": "Error Rate (5xx)",
      "type": "timeseries",
      "datasource": "Prometheus",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 },
      "targets": [
        { "expr": "sum(rate(http_requests_total{status=\"5xx\"}[1m]))", "legendFormat": "5xx/sec", "refId": "A" }
      ]
    }
  ]
}
```

- [ ] **Step 4: Mount the provisioning + dashboards into the `grafana` service in `docker-compose.yml`**

Replace:
```yaml
  grafana:
    image: grafana/grafana:latest
    depends_on: [prometheus]
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
```
with:
```yaml
  grafana:
    image: grafana/grafana:latest
    depends_on: [prometheus]
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards
```

- [ ] **Step 5: Verify with real Docker**

Run:
```bash
docker compose up -d zookeeper kafka redis qdrant api-gateway prometheus grafana
sleep 15
curl -s -u admin:admin http://localhost:3000/api/datasources | grep -o '"name":"Prometheus"'
curl -s -u admin:admin "http://localhost:3000/api/search?query=Lab28" | grep -o '"title":"Lab28 API Gateway"'
```
Expected: both greps print a match.

- [ ] **Step 6: Commit**

```bash
git add monitoring/grafana docker-compose.yml
git commit -m "Auto-provision Grafana Prometheus datasource and API Gateway dashboard"
```

---

### Task 6: Write ANSWERS.md and scaffold screenshots/

**Files:**
- Create: `ANSWERS.md`
- Create: `screenshots/README.md`
- Create: `screenshots/.gitkeep`

- [ ] **Step 1: Create `ANSWERS.md`**

```markdown
# Trả lời 5 câu hỏi nộp bài (SUBMISSION.md)

## 1. Trade-offs kiến trúc: performance vs reliability vs maintainability

Kiến trúc hybrid tách phần rẻ/stateful (Kafka, Qdrant, Redis, Prometheus, Grafana,
Prefect orchestration) chạy local, và phần nặng GPU (vLLM inference) chạy trên
Kaggle free-tier GPU qua tunnel. Đổi lại performance tốt hơn với chi phí gần bằng
0, ta chấp nhận thêm độ trễ mạng và một điểm phụ thuộc bên ngoài (tunnel có thể
rớt, Kaggle session tự tắt sau vài giờ). Về reliability, thay vì thêm retry ở
từng script riêng lẻ, toàn bộ logic chịu lỗi được gom một chỗ trong API Gateway
(`search_context`/`call_llm` trong `api-gateway/main.py`) — Qdrant lỗi thì bỏ
qua context, vLLM lỗi thì trả lời "degraded" thay vì crash. Về maintainability,
Prefect flow dùng `.serve(cron=...)` (tự đăng ký + tự lên lịch trong một
process) thay vì mô hình work-pool/worker/deployment riêng biệt — ít khái niệm
phải đồng bộ hơn, đánh đổi lại là process đó là single point of failure cho
riêng job ingest này (chấp nhận được ở quy mô lab, sẽ cần work-pool thật nếu
scale lên nhiều flow).

## 2. Xử lý ngắt kết nối Local ↔ Kaggle, có fallback không?

Container API Gateway đọc `VLLM_URL`/`EMBED_NGROK_URL` qua `os.environ.get(...,
"")` thay vì `os.environ[...]` — trước đây thiếu biến này làm cả container crash
ngay lúc import, kéo theo cả health check và Prometheus scraping cũng chết theo.
Ở mức request, `call_llm()` bọc lời gọi vLLM trong try/except bắt
`httpx.HTTPError`/`TimeoutException`/`KeyError`, và trả về HTTP 200 với
`"degraded": true` cùng một câu trả lời báo lỗi rõ ràng, thay vì để lỗi 500 lan
ra ngoài. Chưa có cơ chế fallback sang một tunnel/GPU dự phòng thứ hai — đây là
giới hạn đã biết (một Kaggle notebook = một điểm lỗi duy nhất cho phần LLM); nếu
cần production-grade hơn, bước tiếp theo tự nhiên là giữ danh sách nhiều tunnel
URL và thử lần lượt.

## 3. Kafka giúp decouple các components như thế nào?

`scripts/01_ingest_to_kafka.py` (producer) không biết gì về Prefect hay ai đang
consume; `prefect/flows/kafka_to_delta.py` (consumer) không biết ai đã produce
hay có bao nhiêu producer. Nhờ vậy hai phía có thể down/redeploy độc lập —
message chỉ đơn giản nằm chờ trong topic `data.raw` tới lần chạy lịch (mỗi 5
phút, qua `.serve(cron=...)`) tiếp theo. So với gọi thẳng từ ingestion script
vào Prefect, cách này tránh việc ingestion phải chờ Prefect rảnh, và nếu Prefect
chậm/lỗi thì không chặn ngược lại ingestion. Kafka cũng cho khả năng replay tự
nhiên: nếu sửa bug trong `save_to_delta()`, có thể trỏ một consumer group mới
vào topic để xử lý lại toàn bộ message cũ còn nằm đó (nhờ
`auto_offset_reset="earliest"`).

## 4. Observability được implement như thế nào?

- **Logs:** `docker compose logs <service>` theo từng container — đủ dùng ở quy
  mô lab, chưa có log aggregation tập trung (ngoài phạm vi đợt sửa này).
- **Metrics:** `prometheus-fastapi-instrumentator` expose
  `http_requests_total{handler,method,status}` và
  `http_request_duration_seconds_bucket{handler,method,le}` tại
  `/metrics` trên API Gateway; Prometheus scrape đúng 15s/lần. File
  `monitoring/prometheus.yml` trước đây còn scrape cả `kafka:9092` và
  `prefect-orion:4200` — hai target này không hề expose endpoint dạng
  Prometheus nên luôn báo `up=0`; đã bỏ để điểm Observability phản ánh tín hiệu
  thật thay vì nhiễu đỏ vĩnh viễn.
- **Dashboards:** Grafana tự động provision datasource Prometheus và dashboard
  "Lab28 API Gateway" (request rate, P95 latency, error rate 5xx) ngay khi
  `docker compose up`, không cần dựng tay trước mỗi lần demo.
- **Traces:** LangSmith qua `scripts/09_verify_observability.py`, cần
  `LANGCHAIN_API_KEY` thật của học viên; script kiểm tra bằng
  `client.list_runs(project_name="lab28-platform", limit=1)`.

## 5. Nếu Qdrant hoặc Kafka crash, hệ thống xử lý ra sao? Có graceful degradation?

- **Qdrant crash:** `search_context()` bắt `httpx.HTTPError`/`TimeoutException`
  và trả về context rỗng; endpoint `/api/v1/chat` vẫn gọi LLM (không có context
  retrieval) và vẫn trả 200 — chất lượng câu trả lời giảm nhưng dịch vụ không
  down.
- **Kafka crash:** `KafkaConsumer(...)` trong task `consume_and_process` sẽ
  raise khi không connect được; task/flow run đó được Prefect đánh dấu
  "Failed" trong UI, nhưng vì chạy qua `.serve(cron=...)` nên process vẫn tiếp
  tục polling và tự thử lại ở lần chạy 5 phút kế tiếp — một lần Kafka chập chờn
  tự phục hồi mà không cần can thiệp, đánh đổi là mất đúng batch của lần chạy
  đó (không mất dữ liệu vì Kafka vẫn giữ message, chỉ là xử lý bị trễ; chưa có
  cơ chế dead-letter/backoff nào khác ngoài cơ chế retry mặc định của Prefect).
- `GET /health` không phụ thuộc Qdrant/Kafka/vLLM, nên health probe kiểu
  Docker/Kubernetes vẫn báo gateway "up" đúng ngay cả khi các dependency phía
  sau đang degraded.
```

- [ ] **Step 2: Create `screenshots/README.md`**

```markdown
# Screenshots

Chụp và đặt các file sau vào đây trước khi nộp bài (theo `SUBMISSION.md`):

- `prefect_ui.png` — Prefect UI (http://localhost:4200) hiển thị deployment "kafka-to-delta" đang chạy.
- `api_gateway.png` — kết quả `curl http://localhost:8000/health` (hoặc `/api/v1/chat`).
- `grafana_dashboard.png` — Grafana dashboard "Lab28 API Gateway" (http://localhost:3000) có dữ liệu.

Hai file sau đặt ở **thư mục gốc** repo (không phải trong `screenshots/`), đúng
theo cấu trúc trong `SUBMISSION.md`:
- `smoke_tests_results.png` — kết quả `pytest smoke-tests/ -v`
- `production_readiness.png` — kết quả `python scripts/production_readiness_check.py`
```

- [ ] **Step 3: Create `screenshots/.gitkeep`**

Empty file, so the folder is tracked by git before any PNGs exist:
```bash
touch screenshots/.gitkeep
```

- [ ] **Step 4: Verify**

Run: `grep -c "^## [1-5]\." ANSWERS.md`
Expected: `5`.
Run: `git check-ignore -v ANSWERS.md; echo "exit=$?"`
Expected: no output, `exit=1`.

- [ ] **Step 5: Commit**

```bash
git add ANSWERS.md screenshots/README.md screenshots/.gitkeep
git commit -m "Add ANSWERS.md and screenshots/ scaffold"
```

---

### Task 7: Full local-stack verification pass

**Files:**
- Modify: `README.md` (add a short "Local Verification Status" section)

**Interfaces:**
- Consumes: every fix from Tasks 1–6.

- [ ] **Step 1: Bring up the entire local stack**

```bash
docker compose up -d
sleep 30
docker compose ps
```
Expected: every service shows `Up` (not `Restarting`/`Exited`).

- [ ] **Step 2: Run data ingestion**

```bash
pip install -q -r requirements.txt
python scripts/01_ingest_to_kafka.py
```
Expected: prints `Sent: doc_001`, `Sent: doc_002`, `Integration 1 OK: Data → Kafka`.

- [ ] **Step 3: Trigger the Prefect flow run manually (don't wait for the 5-min cron)**

```bash
docker compose exec prefect-runner prefect deployment run 'Kafka to Delta Pipeline/kafka-to-delta'
sleep 20
ls delta-lake/raw/
```
Expected: at least one `batch_*.parquet` file appears on the host (bind-mounted from the container's `/opt/delta-lake/raw`).

- [ ] **Step 4: Push features into Feast (Redis)**

```bash
python scripts/03_delta_to_feast.py
```
Expected: prints `Integration 3+4 OK: Delta Lake → Feast (Redis) — N features stored` with N > 0.

- [ ] **Step 5: Run smoke tests and record actual results**

```bash
pytest smoke-tests/ -v 2>&1 | tee /tmp/smoke_results.txt
```
Record the actual pass/fail count. Expect `test_health_check_passes`, `TestObservability`, `TestFeatureStore` to pass; expect the vLLM-dependent happy-path test and the Qdrant-vector-count test (needs `scripts/05_embed_to_qdrant.py`, which needs a real `EMBED_NGROK_URL`) to fail or be inconclusive here since no real Kaggle endpoint is reachable from this environment.

- [ ] **Step 6: Run the production readiness check and record the score**

```bash
python scripts/production_readiness_check.py 2>&1 | tee /tmp/readiness_results.txt
```
Record the actual score. Expect the "Collection exists" Qdrant check to fail here specifically because `scripts/05_embed_to_qdrant.py` (which creates the `documents` collection) needs the real Kaggle embedding endpoint and hasn't run — this is a known gap, not a bug, and must be closed by the student running the real Kaggle step before their final submission.

- [ ] **Step 7: Add a "Local Verification Status" section to `README.md`**

Append before the "## Nộp Bài" section:
```markdown
## Trạng Thái Verify Local (không cần Kaggle)

Đã verify bằng Docker Compose thật (không mock) trong quá trình chuẩn bị nộp bài:

- [x] Tất cả services local `Up`: `docker compose ps`
- [x] Ingest → Kafka: `scripts/01_ingest_to_kafka.py`
- [x] Kafka → Prefect → Delta Lake: deployment `kafka-to-delta` chạy được, ra file parquet
- [x] Delta Lake → Feast (Redis): `scripts/03_delta_to_feast.py`
- [x] Prometheus scrape đúng `api-gateway` job
- [x] Grafana có datasource + dashboard "Lab28 API Gateway" tự động
- [x] API Gateway trả lời graceful (200 + `degraded: true`) khi Qdrant/vLLM không tới được

**Cần bạn tự chạy trước khi nộp** (cần Kaggle GPU thật, không verify được ở môi
trường chuẩn bị bài này):
- `scripts/05_embed_to_qdrant.py` (cần `EMBED_NGROK_URL` thật)
- Smoke test happy-path gọi LLM thật (cần `VLLM_NGROK_URL` thật)
- `scripts/09_verify_observability.py` phần LangSmith (cần `LANGCHAIN_API_KEY` thật)
- Toàn bộ 5 screenshots theo `screenshots/README.md`
```

- [ ] **Step 8: Commit**

```bash
git add README.md
git commit -m "Document local verification status and remaining Kaggle-dependent steps"
```

---

### Task 8: Automated screenshot capture

**Files:**
- No repo files created for tooling — capture script lives in the scratch/temp directory, only its output PNGs land in the repo.
- Create (output only): `screenshots/prefect_ui.png`, `screenshots/api_gateway.png`, `screenshots/grafana_dashboard.png`, `smoke_tests_results.png` (repo root), `production_readiness.png` (repo root).

**Interfaces:**
- Consumes: the running local stack from Task 7, and `/tmp/smoke_results.txt` / `/tmp/readiness_results.txt` captured there.

- [ ] **Step 1: Confirm Playwright + Chromium are installed**

Run: `python3 -c "from playwright.sync_api import sync_playwright; print('ok')"`
Expected: `ok`. If not installed yet: `pip3 install --user playwright && python3 -m playwright install chromium`.

- [ ] **Step 2: Write the capture script to the scratchpad directory (not committed)**

Write to `/private/tmp/claude-501/-Users-kenz-de-projects-2A202600588-NguyenDucKhang-Day28/d60c9e88-fdc4-4464-b8b3-e2d5d3ac554f/scratchpad/capture_screenshots.py`:
```python
import sys
from pathlib import Path
from playwright.sync_api import sync_playwright

REPO = Path(sys.argv[1])
SCREENSHOTS = REPO / "screenshots"

TERMINAL_CSS = """
body { background:#0d1117; color:#c9d1d9; font-family: 'SF Mono', Menlo, monospace;
       font-size: 14px; padding: 24px; white-space: pre-wrap; }
"""

def render_text_as_image(page, text, out_path, width=1000):
    html = f"<html><head><style>{TERMINAL_CSS}</style></head><body>{text}</body></html>"
    page.set_content(html)
    page.set_viewport_size({"width": width, "height": 100})
    page.screenshot(path=str(out_path), full_page=True)

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900})

        # 1. Prefect UI
        page.goto("http://localhost:4200/deployments")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(1500)
        page.screenshot(path=str(SCREENSHOTS / "prefect_ui.png"))

        # 2. Grafana dashboard (login first)
        page.goto("http://localhost:3000/login")
        page.fill('input[name="user"]', "admin")
        page.fill('input[name="password"]', "admin")
        page.click('button[type="submit"]')
        page.wait_for_timeout(1500)
        page.goto("http://localhost:3000/d/lab28-api-gateway")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(2000)
        page.screenshot(path=str(SCREENSHOTS / "grafana_dashboard.png"))

        # 3. API Gateway curl output, rendered as a terminal-style image
        import subprocess
        health = subprocess.run(
            ["curl", "-s", "http://localhost:8000/health"], capture_output=True, text=True
        ).stdout
        chat = subprocess.run(
            ["curl", "-s", "-X", "POST", "http://localhost:8000/api/v1/chat",
             "-H", "Content-Type: application/json",
             "-d", '{"query": "What is platform engineering?", "embedding": [0.1]}'],
            capture_output=True, text=True,
        ).stdout
        text = (
            f"$ curl http://localhost:8000/health\n{health}\n\n"
            f"$ curl -X POST http://localhost:8000/api/v1/chat ...\n{chat}"
        )
        render_text_as_image(page, text, SCREENSHOTS / "api_gateway.png")

        # 4. Smoke test results
        smoke_text = (REPO / "smoke_results.txt").read_text() if (REPO / "smoke_results.txt").exists() else "(no output captured)"
        render_text_as_image(page, smoke_text, REPO / "smoke_tests_results.png", width=1000)

        # 5. Production readiness results
        readiness_text = (REPO / "readiness_results.txt").read_text() if (REPO / "readiness_results.txt").exists() else "(no output captured)"
        render_text_as_image(page, readiness_text, REPO / "production_readiness.png", width=1000)

        browser.close()

if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Copy the captured text outputs into the repo root as plain text inputs for the script**

```bash
cp /tmp/smoke_results.txt smoke_results.txt
cp /tmp/readiness_results.txt readiness_results.txt
```

- [ ] **Step 4: Run the capture script against the live stack from Task 7**

```bash
python3 /private/tmp/claude-501/-Users-kenz-de-projects-2A202600588-NguyenDucKhang-Day28/d60c9e88-fdc4-4464-b8b3-e2d5d3ac554f/scratchpad/capture_screenshots.py "$(pwd)"
```
Expected: no exceptions; five PNG files created.

- [ ] **Step 5: Verify the files and clean up the intermediate text files**

```bash
ls -la screenshots/*.png smoke_tests_results.png production_readiness.png
rm smoke_results.txt readiness_results.txt
```
Expected: all 5 files listed with non-zero size.

- [ ] **Step 6: Commit the screenshots**

```bash
git add screenshots/prefect_ui.png screenshots/api_gateway.png screenshots/grafana_dashboard.png smoke_tests_results.png production_readiness.png
git commit -m "Add automated screenshots of local stack (Prefect UI, Grafana, API Gateway, smoke tests, readiness check)"
```

---

### Task 9: Final review and push

- [ ] **Step 1: Tear down the local stack**

```bash
docker compose down
```

- [ ] **Step 2: Review the full diff one more time**

```bash
git log --oneline -12
git status
```
Expected: working tree clean, all commits from Tasks 1–8 present.

- [ ] **Step 3: Push to GitHub**

```bash
git push
```
Expected: push succeeds against the existing `origin/main` tracking branch.
