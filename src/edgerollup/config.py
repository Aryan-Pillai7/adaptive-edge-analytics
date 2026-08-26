"""Configuration: environment for *where things are*, YAML for *what to roll up*.

The split is deliberate. Endpoints, credentials and paths change per host and belong in
the environment. Which signals get aggregated, at which granularities, into which
dimensions is the shape of the pipeline itself -- it belongs in version control where a
change to it shows up in a diff.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "rollup.yaml"

SIGNALS = ("metrics", "logs", "traces")


class Settings(BaseSettings):
    """Runtime settings, read from the environment or a local .env file.

    Every value has a working default pointing at the sibling stack's published ports,
    so the job runs against a freshly started `adaptive-edge-otel` with no configuration
    at all.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="",
        extra="ignore",
    )

    # --- where the hot tier lives ------------------------------------------------
    # All three are reached over the host's published ports. Tempo included: its query
    # API is published on 3200 even though its OTLP ingest ports deliberately are not.
    victoriametrics_url: str = "http://localhost:8428"
    loki_url: str = "http://localhost:3100"
    tempo_url: str = "http://localhost:3200"

    # --- where the cold tier lives -----------------------------------------------
    # Parquet archive root. Gitignored: it is generated data, not source.
    parquet_root: Path = REPO_ROOT / "data" / "cold"
    # SQLite checkpoint store. Gitignored for the same reason.
    state_dir: Path = REPO_ROOT / ".rollup-state"

    # --- the hot/cold boundary ---------------------------------------------------
    # Per-signal grace: how long after a bucket closes we wait before considering it
    # sealed and safe to roll up. These are NOT arbitrary -- each covers every buffer
    # between an event being emitted and it being queryable in its backend:
    #
    #   metrics  SDK export 10s + Collector batch 5s + VM latencyOffset 30s
    #   logs     ...plus Loki chunk_idle_period 2m / max_chunk_age 5m
    #   traces   ...plus Tempo max_block_duration 5m + complete_block_timeout 5m
    #
    # A single global value would be either wrong for metrics (needless lag) or
    # actively dangerous for traces: rolling up a window Tempo has not yet made
    # searchable produces an empty result that is indistinguishable from a quiet hour.
    # See decisions.md D-001.
    grace_metrics_seconds: int = Field(default=120, ge=0)
    grace_logs_seconds: int = Field(default=600, ge=0)
    grace_traces_seconds: int = Field(default=900, ge=0)

    # How far back a run will reach when there is no checkpoint yet (first run, or a
    # reset). Bounded so a cold start cannot accidentally attempt to scan the entire
    # retention window of every backend in one pass.
    max_backfill_hours: int = Field(default=24, ge=1)

    # --- HTTP --------------------------------------------------------------------
    http_timeout_seconds: float = Field(default=30.0, gt=0)

    def grace(self, signal: str) -> timedelta:
        """Grace period for a signal, as a timedelta.

        Raises on an unknown signal rather than falling back to a default: a typo in a
        signal name must not silently inherit the shortest grace in the system.
        """
        if signal not in SIGNALS:
            raise ValueError(f"unknown signal {signal!r}; expected one of {SIGNALS}")
        return timedelta(seconds=getattr(self, f"grace_{signal}_seconds"))


def load_rollup_config(path: Path | None = None) -> dict[str, Any]:
    """Load the declarative rollup definitions.

    Kept as a plain dict for now; it gains a typed model in Phase 3 once the aggregation
    layer establishes what a rollup definition actually needs to carry. Typing it before
    then would be guessing at a shape we have not built yet.
    """
    path = path or DEFAULT_CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"rollup config not found: {path}")
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}
