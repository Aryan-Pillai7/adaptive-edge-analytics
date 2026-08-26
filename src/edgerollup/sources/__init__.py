"""Read adapters: one per backend, all producing the same normalised RawRecord.

This layer owns every piece of backend-specific weirdness -- boundary inclusivity,
silent row limits, structured-metadata placement, seconds-vs-nanoseconds -- so that
nothing downstream has to know which backend a record came from.
"""

from edgerollup.sources.base import Source, SourceError, SourceTruncated
from edgerollup.sources.loki import LokiSource
from edgerollup.sources.tempo import TempoSource
from edgerollup.sources.victoriametrics import VictoriaMetricsSource

__all__ = [
    "LokiSource",
    "Source",
    "SourceError",
    "SourceTruncated",
    "TempoSource",
    "VictoriaMetricsSource",
]
