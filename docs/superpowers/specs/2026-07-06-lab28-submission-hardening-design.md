# Lab #28 Submission Hardening — Design

## Context

This repo is a student submission for "Lab #28 — Full Platform Integration Sprint": a hybrid
local (Docker Compose: Kafka, Prefect, Qdrant, Redis, Prometheus, Grafana, FastAPI gateway) +
Kaggle GPU (vLLM, embedding service) AI platform. Graded on Integration Completeness (40%),
Observability (25%), Performance (20%), Architecture Quality (15%), plus 5 written trade-off
questions (see `SUBMISSION.md`).

Repo review surfaced several bugs and gaps that would cost points if submitted as-is. This
design fixes those and adds the missing graded artifacts, without restructuring the repo layout
or building any local stub for the Kaggle/vLLM half (the student will run the real Kaggle
notebook themselves before final submission).

## Goals

- Fix everything that is outright broken (would error or silently fail).
- Add the artifacts `SUBMISSION.md` requires that don't exist yet.
- Hardening the API gateway enough that the answer to written question 5 (graceful degradation)
  is actually true of the code, not aspirational.
- Verify as much as possible locally with real Docker; clearly flag the one piece (live vLLM
  chat completion) that requires the student's own Kaggle + tunnel setup.

## Non-goals

- No repo restructuring into a nested `lab28/` subfolder — current flat layout stays, per
  student decision.
- No local mock/stub server for vLLM or the embedding service committed to the repo. The
  student runs the real Kaggle notebook.
- No new features beyond what's needed to make the existing lab spec (LAB28.md /
  LAB28_GUIDE.md) actually work and score well.

## Changes

### 1. Bug fixes

- **`.gitignore`**: remove the stray trailing `ANSWERS.md` line (it currently silently
  excludes any answers file from git).
- **`.env.example`** (new, repo root): `VLLM_NGROK_URL`, `EMBED_NGROK_URL`,
  `LANGCHAIN_API_KEY`, `LANGCHAIN_PROJECT` placeholders, matching what scripts/README already
  reference.
- **`README.md`**: remove the `cd lab28` step in Quick Start (no such subfolder exists in this
  repo; compose files are at root).
- **`prefect/flows/kafka_to_delta.py`**: `@flow(schedule=...)` is not a valid kwarg on Prefect's
  `@flow` decorator (crashes on import), and `.deploy(work_queue_name=...)` doesn't match the
  pinned Prefect 2.14 API. Fix by pinning a Prefect version where `flow.serve(name=...,
  cron=...)` is available and works against the existing `lab28-worker` process work pool
  already defined in `docker-compose.yml`; verify locally that the flow actually registers and
  runs a scheduled run.
- **`monitoring/prometheus.yml`**: drop the `kafka` and `prefect-orion` scrape jobs — neither
  target exposes a Prometheus-format `/metrics` endpoint, so they'd sit permanently `down` and
  hurt the Observability score for no real benefit. Keep the `api-gateway` job (the only one
  backed by an actual instrumented endpoint).

### 2. Hardening

- **`api-gateway/main.py`**: wrap the Qdrant search call and the vLLM chat-completion call each
  in a timeout + try/except. On failure, return a 200 with a clearly-marked degraded response
  (e.g. `{"answer": "...service temporarily degraded...", "degraded": true}`) instead of an
  unhandled 500. This is what backs up written answer #5 (graceful degradation) and is required
  reading for #2 (Kaggle disconnect handling).
- **Grafana provisioning** (new: `monitoring/grafana/provisioning/datasources/`,
  `monitoring/grafana/provisioning/dashboards/`): auto-provision the Prometheus datasource and
  one dashboard (request rate, P95 latency, error rate panels) so `docker compose up` yields a
  populated dashboard instead of an empty Grafana instance, wired up via a new volume mount in
  `docker-compose.yml`.
- **`screenshots/` folder** (new): `.gitkeep` + short `README.md` listing exactly which
  screenshots go there, matching `SUBMISSION.md`'s expected structure
  (`prefect_ui.png`, `api_gateway.png`, `grafana_dashboard.png`, plus root-level
  `smoke_tests_results.png` and `production_readiness.png`).

### 3. Documentation

- **`ANSWERS.md`** (new, repo root): answers to the 5 required questions from `SUBMISSION.md`,
  grounded in this actual (fixed) architecture — not generic/boilerplate answers. Covers:
  architecture trade-offs, hybrid local/Kaggle disconnect handling & fallback, Kafka's
  decoupling role, the observability implementation (logs/metrics/traces), and graceful
  degradation on service crash (Qdrant/Kafka).

## Verification plan

Using local Docker (available in this environment), bring up the local stack
(`docker compose up -d`) and confirm:

- All local services report `Up` / healthy.
- `scripts/01_ingest_to_kafka.py` successfully produces to Kafka.
- The Prefect flow deploys and runs a scheduled consume→save-to-parquet cycle without crashing.
- `scripts/03_delta_to_feast.py` pushes features into Redis.
- Prometheus successfully scrapes `api-gateway` (post scrape-config fix).
- Grafana comes up with the Prometheus datasource and dashboard pre-provisioned.
- `api-gateway` returns a graceful degraded response (not a 500) when Qdrant or the vLLM URL is
  unreachable.
- `pytest smoke-tests/ -v` and `scripts/production_readiness_check.py` — expect the vLLM/embedding-
  dependent checks (the chat "happy path" test, and `05_embed_to_qdrant.py`) to fail or be
  skipped here since no real Kaggle endpoint is reachable from this environment; everything else
  should pass.

What the student must still do before final submission: run the real Kaggle notebook, get the
tunnel URL, put it in `.env`, rerun the vLLM-dependent smoke tests, and take the required
screenshots (Prefect UI, API Gateway call, Grafana dashboard, smoke test results, readiness
score).

## Risks / open questions

- Prefect version pin change: need to confirm during implementation which minor version
  actually supports `flow.serve(cron=...)` cleanly against a `process`-type work pool without
  requiring further compose changes; if a newer minor version shifts other behavior, the compose
  file's `prefect-worker` command may need a matching small adjustment.
- The degraded-response contract for the API Gateway changes `smoke-tests/test_e2e.py`'s
  behavior expectations for `TestFailurePath` — need to make sure the existing test assertions
  (`invalid request → 422`, `timeout doesn't crash health`) still hold after adding try/except,
  and the new "degraded" branch doesn't accidentally mask genuine 4xx errors like the missing-
  field case.
