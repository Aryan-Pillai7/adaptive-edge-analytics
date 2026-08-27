"""Builds source adapters from settings.

A single place that knows which backend serves which signal. Everything else -- the CLI,
the pipeline, the integration tests -- asks here rather than constructing adapters
itself, so adding a fourth signal touches this file and nothing that consumes it.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager

import httpx

from edgerollup.config import Settings, load_rollup_config
from edgerollup.rollups import MetricsRollup, Rollup
from edgerollup.sinks import ParquetSink, Sink, VictoriaMetricsSink
from edgerollup.sources import LokiSource, Source, TempoSource, VictoriaMetricsSource
from edgerollup.writer import RollupWriter

log = logging.getLogger(__name__)


@contextmanager
def open_sources(settings: Settings) -> Iterator[dict[str, Source]]:
    """Yield one adapter per signal, sharing one HTTP client.

    Shared because all three backends are on localhost and a per-adapter client would
    mean three connection pools for what is, in practice, three ports on one host.
    """
    config = load_rollup_config()

    def selector(signal: str, fallback: str) -> str:
        configured = (config["signals"].get(signal) or {}).get("selector")
        if not configured:
            # Loud, because the fallback for metrics and logs does NOT exclude the cold
            # tier, and a job reading its own output produces plausible nonsense rather
            # than an error.
            log.warning(
                "%s: no selector in rollup.yaml — falling back to %r, which does not "
                "exclude the cold tier",
                signal,
                fallback,
            )
            return fallback
        return configured

    with httpx.Client(timeout=settings.http_timeout_seconds) as client:
        yield {
            "metrics": VictoriaMetricsSource(
                settings.victoriametrics_url, client, selector("metrics", '{__name__!=""}')
            ),
            "logs": LokiSource(settings.loki_url, client, selector("logs", '{service_name=~".+"}')),
            "traces": TempoSource(settings.tempo_url, client, selector("traces", "{}")),
        }


#: Which rollup implements which signal. Logs and traces join as they are built; a
#: signal with no entry is simply not rolled up yet, which `build_writers` reports
#: rather than failing on.
ROLLUPS: dict[str, type[Rollup]] = {
    "metrics": MetricsRollup,
}


def dimensions_for(signal: str, config: dict | None = None) -> tuple[str, ...]:
    """The dimension keys a signal groups by: the shared set plus its own."""
    config = config or load_rollup_config()
    common = tuple(config.get("common_dimensions") or ())
    specific = tuple((config["signals"].get(signal) or {}).get("dimensions") or ())
    # Deduplicated but order-stable, so a config listing a dimension in both places does
    # not produce it twice.
    seen: dict[str, None] = {}
    for key in common + specific:
        seen[key] = None
    return tuple(seen)


@contextmanager
def open_sinks(settings: Settings) -> Iterator[dict[str, list[Sink]]]:
    """Sinks per signal, in write order.

    Parquet is always first: it is the authoritative copy and the only one that replaces
    atomically on rewrite. See writer.py for why the order matters.
    """
    config = load_rollup_config()
    with httpx.Client(timeout=settings.http_timeout_seconds) as client:
        parquet = ParquetSink(settings.parquet_root)
        victoria = VictoriaMetricsSink(settings.victoriametrics_url, client)
        available: dict[str, Sink] = {"parquet": parquet, "victoriametrics": victoria}

        per_signal: dict[str, list[Sink]] = {}
        for signal, entry in config["signals"].items():
            names = list(entry.get("sinks") or [])
            # Config order is not trusted for the authoritative-first rule -- sorting
            # puts parquet ahead of victoriametrics regardless of how the YAML is
            # written, so the invariant cannot be broken by editing config.
            names.sort(key=lambda n: 0 if n == "parquet" else 1)
            per_signal[signal] = [available[n] for n in names if n in available]
        yield per_signal


def build_writer(signal: str, sinks: list[Sink]) -> RollupWriter | None:
    """The processor for a signal, or None if that signal has no rollup yet."""
    rollup_type = ROLLUPS.get(signal)
    if rollup_type is None:
        return None
    return RollupWriter(rollup_type(dimensions_for(signal)), sinks)
