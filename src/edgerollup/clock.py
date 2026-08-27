"""The single source of "now".

Nothing else in this package calls `datetime.now()`. Every decision about whether a
bucket is sealed, whether a claim has expired, or how far back to reach on a cold start
routes through a Clock that is passed in.

This is not test scaffolding for its own sake. The correctness of this pipeline is
almost entirely a property of time arithmetic, and time arithmetic that reads a hidden
global can only be tested by waiting -- which means the boundary conditions that matter
most (a bucket one second inside its grace period, a lease that expires mid-run) would
either go untested or make the suite take hours.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        """The current instant, always timezone-aware UTC."""
        ...


class SystemClock:
    """Wall-clock time. The only implementation used in production."""

    def now(self) -> datetime:
        return datetime.now(UTC)


class FrozenClock:
    """A clock that only moves when told to.

    Used by the boundary tests to sit exactly on the edges that matter: the instant a
    bucket becomes sealed, the instant a lease expires, the instant before each.
    """

    def __init__(self, start: datetime) -> None:
        if start.tzinfo is None:
            raise ValueError("FrozenClock must be given a timezone-aware instant")
        self._now = start.astimezone(UTC)

    def now(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> datetime:
        """Move forward. Refuses to go backwards.

        Time going backwards in a batch pipeline means a bucket can be sealed on one
        run and unsealed on the next, which no checkpoint scheme can survive. If a test
        wants that scenario it has to construct it deliberately rather than reach it by
        passing a negative delta.
        """
        if delta < timedelta(0):
            raise ValueError("FrozenClock.advance does not go backwards")
        self._now = self._now + delta
        return self._now

    def set(self, moment: datetime) -> datetime:
        if moment.tzinfo is None:
            raise ValueError("FrozenClock.set requires a timezone-aware instant")
        self._now = moment.astimezone(UTC)
        return self._now
