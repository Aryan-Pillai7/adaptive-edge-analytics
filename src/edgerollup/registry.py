"""Builds source adapters from settings.

A single place that knows which backend serves which signal. Everything else -- the CLI,
the pipeline, the integration tests -- asks here rather than constructing adapters
itself, so adding a fourth signal touches this file and nothing that consumes it.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import httpx

from edgerollup.config import Settings
from edgerollup.sources import LokiSource, Source, TempoSource, VictoriaMetricsSource


@contextmanager
def open_sources(settings: Settings) -> Iterator[dict[str, Source]]:
    """Yield one adapter per signal, sharing one HTTP client.

    Shared because all three backends are on localhost and a per-adapter client would
    mean three connection pools for what is, in practice, three ports on one host.
    """
    with httpx.Client(timeout=settings.http_timeout_seconds) as client:
        yield {
            "metrics": VictoriaMetricsSource(settings.victoriametrics_url, client),
            "logs": LokiSource(settings.loki_url, client),
            "traces": TempoSource(settings.tempo_url, client),
        }
