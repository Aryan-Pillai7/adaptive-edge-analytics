"""Write adapters for the cold tier, and the rule that governs writing to both."""

from edgerollup.sinks.base import Sink, SinkError
from edgerollup.sinks.loki import LokiSink
from edgerollup.sinks.parquet import ParquetSink
from edgerollup.sinks.victoriametrics import VictoriaMetricsSink

__all__ = ["LokiSink", "ParquetSink", "Sink", "SinkError", "VictoriaMetricsSink"]
