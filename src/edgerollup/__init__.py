"""Retention tiering and rollup pipeline for the adaptive-edge-otel storage backends.

Reads raw (hot-tier) telemetry from VictoriaMetrics, Loki and Tempo, aggregates it into
coarser time buckets, and writes rolled-up (cold-tier) summaries back alongside a local
Parquet archive.

The package is laid out as three layers with a deliberate one-way dependency:

    sources/  ->  rollups/  ->  sinks/
    (read I/O)   (pure agg)     (write I/O)

``rollups`` performs no I/O at all. That is what makes the aggregation logic testable
without a running stack, and what keeps "add a new signal" or "add a new granularity"
to one new file plus a config entry.
"""

__version__ = "0.1.0"
