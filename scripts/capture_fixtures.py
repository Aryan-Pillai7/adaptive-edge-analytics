"""Capture real backend responses into tests/fixtures/.

Fixtures in this project are recorded, never hand-written. A hand-written fixture
encodes what we *think* an API returns, and every genuinely expensive surprise so far --
VictoriaMetrics' inclusive window ends, Loki merging structured metadata into the stream
object, Tempo's overlap matching, Loki rejecting `limit` above its own cap -- was a case
of the real response differing from a reasonable expectation. A test built on a guess
would have confirmed the guess.

Run against a live stack when a backend is upgraded, or when a new field is needed:

    ./.venv/Scripts/python.exe scripts/capture_fixtures.py

Then inspect the diff. A change here is a change in what the pipeline has to parse, and
it should be reviewed as such rather than committed blind.
"""

from __future__ import annotations

import json
import pathlib
import sys
import time

import httpx

ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"

VM = "http://localhost:8428"
LOKI = "http://localhost:3100"
TEMPO = "http://localhost:3200"

# Wide enough to catch whatever the stack has been doing recently, since these are
# shape fixtures -- the exact values do not matter, the structure does.
LOOKBACK_SECONDS = 6 * 3600


def write(name: str, content: str) -> None:
    path = FIXTURES / name
    path.write_text(content, encoding="utf-8", newline="\n")
    print(f"  wrote {path.relative_to(ROOT)}  ({len(content)} bytes)")


def main() -> int:
    FIXTURES.mkdir(parents=True, exist_ok=True)
    now = int(time.time())
    start = now - LOOKBACK_SECONDS
    client = httpx.Client(timeout=60.0)

    print("victoriametrics /api/v1/export")
    r = client.get(
        f"{VM}/api/v1/export",
        params={"match[]": "edgeapp_requests_total", "start": start, "end": now},
    )
    r.raise_for_status()
    # Kept as raw JSON-lines rather than re-serialised: the streaming line format IS the
    # thing being tested, and pretty-printing it would test a format we never receive.
    write("vm_export.jsonl", r.text)

    print("loki /loki/api/v1/query_range")
    r = client.get(
        f"{LOKI}/loki/api/v1/query_range",
        params={
            "query": '{service_name=~".+"}',
            "start": str(start * 10**9),
            "end": str(now * 10**9),
            "limit": 200,
            "direction": "forward",
        },
    )
    r.raise_for_status()
    write("loki_query_range.json", json.dumps(r.json(), indent=2, sort_keys=True) + "\n")

    print("tempo /api/search")
    for label, query in (
        ("tempo_search.json", "{}"),
        ("tempo_search_errors.json", "{ status = error }"),
    ):
        r = client.get(
            f"{TEMPO}/api/search",
            params={"q": query, "start": start, "end": now, "limit": 200},
        )
        r.raise_for_status()
        write(label, json.dumps(r.json(), indent=2, sort_keys=True) + "\n")

    print("\nDone. Review the diff before committing — a change here is a change in the")
    print("shape the source adapters have to parse.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
