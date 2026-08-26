"""Loki: log entries via /loki/api/v1/query_range.

Loki is the one backend here whose window convention is already correct -- verified
directly: splitting a window at T put the entry at T in the right-hand half only, with
zero overlap and zero loss. `Source.read()` still applies its half-open filter, because
a guarantee that holds because of a backend's current behaviour is not a guarantee.

The two things that *do* need care:

**Silent truncation.** `limit` caps the response and nothing in the body says it was
applied -- asking for 2 entries out of a larger window returns exactly 2, with no flag,
no count, and the same structure as a complete answer. Detected here by asking for one
row more than we intend to accept, and handled by `Source.read()` bisecting the window.

**dedup_count.** The upstream Collector's `log_dedup` processor collapses identical log
records and records how many it collapsed. So one entry here can represent thousands of
real log events, and counting rows would undercount a log flood by exactly the dedup
factor -- which the upstream project works hard to make enormous. `value` carries
`dedup_count`, not 1. This is the difference between "25 log records stored" and the
"15,001 events they represent" in the sibling's fidelity table.

Note also that severity is Loki STRUCTURED METADATA, not an indexed label. It arrives
merged into the `stream` object alongside real labels, so parsing reads it from there --
but it cannot be selected on in the LogQL matcher.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from edgerollup.model import RawRecord, TimeRange, canonical_dimensions, stable_identity
from edgerollup.sources.base import Source, SourceError

log = logging.getLogger(__name__)

# Loki enforces `max_entries_limit_per_query` (default 5000) as a HARD limit and rejects
# anything above it with a 400 -- it does not clamp. Since the truncation probe below
# asks for row_limit + 1, row_limit must sit one BELOW Loki's cap or every query fails
# with "max entries limit per query exceeded (5001 > 5000)". Found by running it.
LOKI_MAX_ENTRIES = 5000
DEFAULT_ROW_LIMIT = LOKI_MAX_ENTRIES - 1

# Per-record noise that would explode cold-tier cardinality if kept as dimensions.
# service_instance_id is the important one (decisions.md D-005) but the code_* and
# *_timestamp fields are per-record too and equally unsuitable for grouping.
_NOISE_FIELDS = frozenset(
    {
        "service_instance_id",
        "observed_timestamp",
        "first_observed_timestamp",
        "last_observed_timestamp",
        "trace_id",
        "span_id",
        "flags",
        "dedup_count",
        "order_id",
        "code_file_path",
        "code_function_name",
        "code_line_number",
    }
)


class LokiSource(Source):
    signal = "logs"
    row_limit = DEFAULT_ROW_LIMIT

    def __init__(self, base_url: str, client, selector: str = '{service_name=~".+"}') -> None:
        super().__init__(base_url, client)
        self.selector = selector

    def health(self) -> bool:
        try:
            return self.client.get(f"{self.base_url}/ready").status_code == 200
        except Exception:
            return False

    def _fetch(self, window: TimeRange) -> tuple[list[RawRecord], bool]:
        # Ask for one MORE than we are willing to accept. If that extra row comes back,
        # the window definitely holds more than row_limit and must be bisected. Asking
        # for exactly row_limit cannot distinguish "exactly this many exist" from "at
        # least this many exist", and guessing wrong loses data silently.
        probe_limit = self.row_limit + 1

        response = self._get(
            "/loki/api/v1/query_range",
            {
                "query": self.selector,
                # Loki takes nanoseconds. Built from integer arithmetic on the epoch
                # rather than float seconds: float64 cannot represent nanosecond
                # precision at current epoch values, so the conversion would round and
                # records near a boundary would land in the wrong bucket.
                "start": _to_nanos(window.start),
                "end": _to_nanos(window.end),
                "limit": probe_limit,
                # Ascending. With `backward` (Loki's default) a truncated page keeps the
                # NEWEST entries, so bisecting would recurse on halves whose oldest data
                # had already been dropped.
                "direction": "forward",
            },
        )

        payload = response.json()
        data = payload.get("data") or {}
        result_type = data.get("resultType")
        if result_type != "streams":
            raise SourceError(
                f"logs: expected resultType 'streams', got {result_type!r}. The selector "
                f"{self.selector!r} may be an aggregation rather than a stream query."
            )

        records: list[RawRecord] = []
        for stream in data.get("result") or []:
            records.extend(self._records_for_stream(stream))

        return records, len(records) >= probe_limit

    def _records_for_stream(self, stream: dict) -> list[RawRecord]:
        # Loki merges structured metadata into this object alongside genuine index
        # labels, so severity_text and dedup_count are read from here -- and every
        # distinct metadata combination arrives as its own "stream".
        labels: dict[str, str] = stream.get("stream") or {}
        dimensions = canonical_dimensions(
            {k: v for k, v in labels.items() if k not in _NOISE_FIELDS}
        )
        dedup_count = _positive_int(labels.get("dedup_count"), default=1)

        out: list[RawRecord] = []
        for entry in stream.get("values") or []:
            # [timestamp_ns, line], with a third structured-metadata element in some
            # Loki versions. Indexed positionally rather than unpacked, so a third
            # element does not raise.
            if len(entry) < 2:
                raise SourceError(f"logs: malformed entry {entry!r}")
            ts_nanos, line = str(entry[0]), str(entry[1])

            out.append(
                RawRecord(
                    signal="logs",
                    timestamp=_from_nanos(ts_nanos),
                    dimensions=dimensions,
                    # Events represented, not rows returned. See module docstring.
                    value=float(dedup_count),
                    # Timestamp alone is not unique -- Loki happily stores several
                    # entries at one nanosecond, and the flood in the sibling project
                    # produces exactly that. The line and the full label set are needed
                    # to tell two simultaneous records apart.
                    identity=stable_identity(ts_nanos, line, tuple(sorted(labels.items()))),
                    signal_kind=labels.get("severity_text") or labels.get("detected_level") or "",
                )
            )
        return out


def _to_nanos(moment: datetime) -> str:
    """Epoch nanoseconds as a string, via integer maths only."""
    return str(int(moment.timestamp() * 1_000_000) * 1_000)


def _from_nanos(value: str) -> datetime:
    nanos = int(value)
    # Split before converting: passing nanos/1e9 to fromtimestamp loses sub-millisecond
    # precision to float rounding, which is enough to move a record across a boundary.
    seconds, remainder = divmod(nanos, 1_000_000_000)
    return datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=remainder // 1_000)


def _positive_int(value: object, default: int) -> int:
    """Parse a count, falling back rather than failing.

    A missing or malformed dedup_count means "this record represents itself", which is
    the correct reading for any log that never went through the dedup processor.
    """
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default
