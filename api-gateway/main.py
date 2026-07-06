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
