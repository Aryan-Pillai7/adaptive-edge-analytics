"""Aggregation. No I/O, no network, no clock.

Everything in this package is a pure function from records to rows. That is what lets
the aggregation be tested against recorded fixtures with nothing running, and what keeps
"add a signal" or "add a granularity" to one new module plus a config entry.
"""

from edgerollup.rollups.base import Rollup
from edgerollup.rollups.metrics import MetricsRollup

__all__ = ["MetricsRollup", "Rollup"]
