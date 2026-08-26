"""Fixtures for tests that run against the live adaptive-edge-otel stack.

Start it first:  bash scripts/up.sh  (in the sibling repo)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from edgerollup.config import Settings
from edgerollup.model import TimeRange
from edgerollup.registry import open_sources


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope="session")
def sources(settings: Settings):
    with open_sources(settings) as adapters:
        yield adapters


@pytest.fixture(scope="session", autouse=True)
def require_stack(sources):
    """Skip the whole suite -- loudly -- if the stack is not up.

    Skipping beats failing: a missing stack is a setup problem, not a defect in the read
    path, and a wall of connection errors would bury the one line that says so.
    """
    down = [name for name, source in sources.items() if not source.health()]
    if down:
        pytest.skip(
            f"backends unreachable: {', '.join(down)}. "
            f"Start the sibling stack with `bash scripts/up.sh` in adaptive-edge-otel."
        )


# How far back to hunt for a populated window before giving up. Bounded by the tightest
# hot retention in the stack (Tempo's 24h block_retention) -- looking further back can
# only ever find nothing.
MAX_LOOKBACK_HOURS = 24
WINDOW = timedelta(hours=1)


@pytest.fixture(scope="session")
def settled_window(settings: Settings, sources) -> dict[str, tuple[datetime, datetime]]:
    """The most recent one-hour window per signal that is BOTH settled and populated.

    Two constraints, and both matter.

    *Settled*: every assertion here is about whether two reads AGREE, so the window must
    not race ingestion. Reading up to `now` makes a difference between two reads
    ambiguous -- data arriving between the two calls is indistinguishable from a
    boundary bug, and the test flakes in exactly the way that trains people to ignore
    it. So the search starts a full two grace periods in the past.

    *Populated*: an empty window passes every property in this suite trivially. A gate
    that skips itself when the stack is quiet is not a gate, and traces are the signal
    most likely to be quiet -- so the window is hunted for rather than assumed.
    """
    now = datetime.now(UTC)
    windows: dict[str, tuple[datetime, datetime]] = {}

    for signal, source in sources.items():
        # Two grace periods, not one: the margin has to cover the grace itself plus the
        # time this suite spends running.
        newest_end = now - settings.grace(signal) * 2
        for step in range(MAX_LOOKBACK_HOURS):
            end = newest_end - WINDOW * step
            if source.read(TimeRange(end - WINDOW, end)):
                windows[signal] = (end - WINDOW, end)
                break

    return windows
