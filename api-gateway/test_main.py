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
