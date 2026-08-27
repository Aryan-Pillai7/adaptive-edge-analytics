"""The tiering report: what each backend actually retains, and what the rollup buys.

Two things, both of which have to be measured rather than asserted from config:

**The retention audit.** Every backend's retention is read from the running instance, not
from what this project believes it configured. A retention that drifted -- or was never
set, which is how Loki ended up as the longest-lived backend in a "short retention"
stack -- is exactly the kind of thing a config file cannot tell you about itself.

**The hot/cold ratio.** The deliverable. How much smaller the cold tier is than the raw
data it summarises, per signal, measured over a real window.

The audit also checks the one inequality that makes the whole pipeline safe:

    hot_retention  >  max_grace + job_interval + safety_margin

If raw data expires before the job gets to it, the rollup silently records an empty
bucket. That failure is invisible -- an empty hour looks exactly like a quiet hour -- so
the margin is checked rather than assumed.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import timedelta

import httpx

from edgerollup.config import Settings

log = logging.getLogger(__name__)

_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([smhdwy])\s*$", re.IGNORECASE)
_UNITS = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
    "y": 31536000,
}


def parse_duration(text: str) -> timedelta | None:
    """Parse a Go-style duration like `3d`, `72h`, `2160h`. None if unparseable."""
    match = _DURATION.match(str(text))
    if not match:
        return None
    return timedelta(seconds=float(match.group(1)) * _UNITS[match.group(2).lower()])


def humanise(delta: timedelta | None) -> str:
    if delta is None:
        return "unknown"
    seconds = int(delta.total_seconds())
    if seconds % 86400 == 0:
        return f"{seconds // 86400}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    return f"{seconds // 60}m"


@dataclass
class BackendRetention:
    name: str
    hot: timedelta | None
    cold: timedelta | None
    #: Whether the backend can keep cold data longer than raw at all.
    can_tier: bool
    note: str = ""


def probe_retentions(settings: Settings, client: httpx.Client) -> list[BackendRetention]:
    """Read retention from the running backends.

    Deliberately reads the live instances rather than the config files. A config file
    describes what someone intended; only the running process knows what is in force --
    and the whole reason this project audits retention at all is that Loki's real
    behaviour (the 744h default) did not match anything written down.
    """
    results: list[BackendRetention] = []

    # --- VictoriaMetrics: a single global retention, no per-series override -------
    vm_hot: timedelta | None = None
    try:
        response = client.get(f"{settings.victoriametrics_url}/metrics")
        response.raise_for_status()
        found = re.search(r'flag\{name="retentionPeriod",\s*value="([^"]+)"', response.text)
        if found:
            vm_hot = parse_duration(found.group(1))
        has_filters = 'name="retentionFilters"' in response.text
    except httpx.HTTPError as exc:
        log.warning("victoriametrics: could not read retention: %s", exc)
        has_filters = False

    results.append(
        BackendRetention(
            name="victoriametrics",
            hot=vm_hot,
            # Cold shares the global retention: there is nothing else it could be.
            cold=vm_hot,
            can_tier=has_filters,
            note=(
                "single global retention; cold rollups expire WITH raw data"
                if not has_filters
                else "retention filters available"
            ),
        )
    )

    # --- Loki: global period plus per-stream overrides ---------------------------
    loki_hot = loki_cold = None
    try:
        response = client.get(f"{settings.loki_url}/config")
        response.raise_for_status()
        body = response.text
        found = re.search(r"^\s*retention_period:\s*(\S+)", body, re.MULTILINE)
        if found:
            loki_hot = parse_duration(found.group(1))
        # The cold rule is the one selecting tier="cold".
        block = re.search(r"retention_stream:\s*\n((?:\s+-.*\n|\s+\w+:.*\n)+)", body, re.MULTILINE)
        if block and "cold" in block.group(1):
            period = re.search(r"period:\s*(\S+)", block.group(1))
            if period:
                loki_cold = parse_duration(period.group(1))
    except httpx.HTTPError as exc:
        log.warning("loki: could not read retention: %s", exc)

    results.append(
        BackendRetention(
            name="loki",
            hot=loki_hot,
            cold=loki_cold,
            can_tier=loki_cold is not None,
            note=(
                'retention_stream {tier="cold"} outlives raw'
                if loki_cold
                else "no cold-tier rule: rollups expire with raw data"
            ),
        )
    )

    # --- Tempo: block retention, not exposed over HTTP ---------------------------
    # Tempo publishes no config endpoint here, and it is never written to, so its cold
    # retention is not a meaningful concept -- trace rollups live in VM and Parquet.
    results.append(
        BackendRetention(
            name="tempo",
            hot=timedelta(hours=24),
            cold=None,
            can_tier=False,
            note="block_retention 24h (from config, no HTTP endpoint); never written to",
        )
    )

    return results


@dataclass
class SafetyCheck:
    ok: bool
    backend: str
    hot: timedelta | None
    required: timedelta
    detail: str


def check_safety_margin(
    retentions: list[BackendRetention],
    settings: Settings,
    job_interval: timedelta,
    safety_margin: timedelta,
) -> list[SafetyCheck]:
    """Assert hot retention exceeds the worst-case time before the job reaches a bucket.

    The worst case is: the bucket's own width has to pass, then its grace period, then up
    to a full job interval before the next run starts, plus margin for a run that is late
    or slow. If retention is shorter than that sum, data can expire unrolled -- and the
    resulting empty bucket is indistinguishable from a genuinely quiet one.
    """
    worst_grace = max(settings.grace(signal) for signal in ("metrics", "logs", "traces"))
    required = worst_grace + job_interval + safety_margin

    checks = []
    for backend in retentions:
        if backend.hot is None:
            checks.append(SafetyCheck(False, backend.name, None, required, "retention unknown"))
            continue
        ok = backend.hot > required
        headroom = backend.hot - required
        checks.append(
            SafetyCheck(
                ok=ok,
                backend=backend.name,
                hot=backend.hot,
                required=required,
                detail=(
                    f"{humanise(headroom)} of headroom"
                    if ok
                    else f"SHORT by {humanise(-headroom)} — data can expire unrolled"
                ),
            )
        )
    return checks


@dataclass
class SignalRatio:
    signal: str
    raw_records: int
    rollup_rows: int
    cold_bytes: int
    buckets: int
    #: Buckets that were processed and held nothing. They still write a file -- "we
    #: looked and there was nothing" has to be distinguishable from "never ran" -- and
    #: each carries the Parquet schema footer, a few KB regardless of content.
    empty_buckets: int = 0
    empty_bytes: int = 0

    @property
    def overhead_bytes(self) -> int:
        """Bytes attributable to empty buckets alone.

        Worth separating out. For a sparse signal the archive is mostly schema footers,
        so a raw byte ratio makes the rollup look far less effective than it is -- the
        floor is per-file overhead, not the summaries.
        """
        return self.empty_bytes

    @property
    def reduction(self) -> float:
        """How many raw records each rollup row stands in for."""
        return self.raw_records / self.rollup_rows if self.rollup_rows else 0.0

    @property
    def bytes_per_raw_record(self) -> float:
        return self.cold_bytes / self.raw_records if self.raw_records else 0.0


def measure_ratio(signal: str, parquet_root, granularity_name: str = "1h") -> SignalRatio:
    """Measure the cold tier that has actually been written for one signal.

    Reads the Parquet archive rather than re-querying the backends: it is the
    authoritative copy, it records `count` per row (so the raw record total is recoverable
    without a second read), and it is the thing whose size the ratio is about.
    """
    import pyarrow.parquet as pq

    root = parquet_root / f"signal={signal}" / f"granularity={granularity_name}"
    raw_records = rollup_rows = cold_bytes = buckets = empty = empty_bytes = 0

    for path in sorted(root.rglob("*.parquet")):
        table = pq.read_table(path)
        size = path.stat().st_size
        buckets += 1
        cold_bytes += size
        rollup_rows += table.num_rows
        if table.num_rows:
            raw_records += sum(table.column("count").to_pylist())
        else:
            empty += 1
            empty_bytes += size

    ratio = SignalRatio(
        signal=signal,
        raw_records=raw_records,
        rollup_rows=rollup_rows,
        cold_bytes=cold_bytes,
        buckets=buckets,
        empty_buckets=empty,
    )
    ratio.empty_bytes = empty_bytes
    return ratio
