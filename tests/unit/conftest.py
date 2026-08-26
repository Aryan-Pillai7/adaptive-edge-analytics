"""Fixture loading and offline HTTP stubs.

Unit tests never touch the network. The adapters are exercised through httpx's own
MockTransport rather than by monkeypatching their methods, so the code under test builds
real requests, parses real responses, and takes the same code path it takes in
production -- the only difference being where the bytes come from.
"""

from __future__ import annotations

import json
import pathlib

import httpx
import pytest

FIXTURES = pathlib.Path(__file__).resolve().parents[1] / "fixtures"


def load_text(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def load_json(name: str) -> dict:
    return json.loads(load_text(name))


@pytest.fixture
def vm_export_text() -> str:
    return load_text("vm_export.jsonl")


@pytest.fixture
def loki_response() -> dict:
    return load_json("loki_query_range.json")


@pytest.fixture
def tempo_search() -> dict:
    return load_json("tempo_search.json")


@pytest.fixture
def tempo_search_errors() -> dict:
    return load_json("tempo_search_errors.json")


def stub_client(handler) -> httpx.Client:
    """An httpx.Client that answers from `handler` instead of the network.

    `handler` receives the real httpx.Request, so a test can assert on the exact query
    parameters the adapter built -- which is how the boundary and limit behaviour gets
    verified without a live backend.
    """
    return httpx.Client(transport=httpx.MockTransport(handler))


def json_responder(payload: dict, record: list | None = None):
    def handler(request: httpx.Request) -> httpx.Response:
        if record is not None:
            record.append(request)
        return httpx.Response(200, json=payload)

    return handler


def text_responder(body: str, record: list | None = None):
    def handler(request: httpx.Request) -> httpx.Response:
        if record is not None:
            record.append(request)
        return httpx.Response(200, text=body)

    return handler
