"""The Source contract, and the two guarantees every adapter inherits.

Every backend in this stack gets a window wrong in its own way, and every one of them
does it *silently*:

  * VictoriaMetrics treats both ends of a window as inclusive, so splitting a window at
    T returns the sample at T in both halves.
  * Loki and Tempo cap results at a row limit and return the truncated page with no flag
    saying so -- the response for "give me everything" and "here is the first 100 of
    4,000" are structurally identical.
  * Tempo matches any trace that *overlaps* the window rather than one that starts in
    it, so a trace spanning a boundary is returned on both sides.

Every one of those produces a plausible wrong number rather than an error. So the two
guarantees below are enforced here, once, for all adapters -- rather than being
re-remembered in three separate files:

  1. **Half-open.** Whatever a backend's own convention is, `read()` returns only
     records whose timestamp falls in ``[start, end)``.
  2. **Complete.** If a fetch may have been truncated, the window is bisected and
     re-read until every part comes back under the limit. Never a partial answer that
     looks whole.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections import Counter
from datetime import timedelta

import httpx

from edgerollup.model import RawRecord, TimeRange

log = logging.getLogger(__name__)

# Below this, bisecting to escape a row limit is futile -- a window this small returning
# a full page means the data is genuinely denser than the limit, not that the window is
# too wide. Failing beats silently truncating.
MIN_BISECT = timedelta(seconds=2)


class SourceError(RuntimeError):
    """A backend could not be read."""


class SourceTruncated(SourceError):
    """A backend capped the result and the window cannot be subdivided any further.

    Deliberately fatal. The alternative -- returning what we got -- writes a rollup that
    is quietly missing an unknown fraction of its input, which is the single hardest
    class of bug to notice downstream.
    """


class Source(ABC):
    """Reads raw records for one signal out of one backend.

    Subclasses implement `_fetch`; everything else is enforced here.
    """

    #: Which signal this adapter produces ("metrics" | "logs" | "traces").
    signal: str = ""
    #: Max rows requested per call. Hitting exactly this many is how truncation is
    #: detected, since no backend here reports it.
    row_limit: int = 5000

    def __init__(self, base_url: str, client: httpx.Client) -> None:
        self.base_url = base_url.rstrip("/")
        self.client = client

    # --- the contract subclasses implement ------------------------------------
    @abstractmethod
    def _fetch(self, window: TimeRange) -> tuple[list[RawRecord], bool]:
        """Fetch one page for `window`.

        Returns the records (which MAY violate half-open -- that gets fixed here) and
        whether the result might have been truncated.
        """

    def health(self) -> bool:
        """Whether the backend answers at all. Used by the integration suite to skip
        loudly rather than fail with a wall of connection errors."""
        try:
            return self.client.get(f"{self.base_url}/").status_code < 500
        except httpx.HTTPError:
            return False

    # --- the guarantees every adapter gets ------------------------------------
    def read(self, window: TimeRange) -> list[RawRecord]:
        """Every record whose timestamp falls in `window`, exactly once.

        The ordering here matters. Truncation is judged on the RAW page size, before
        half-open filtering: a page that came back full is suspect even if filtering
        happens to discard some of it, because the discarded rows are not evidence about
        what the backend left out.
        """
        records, maybe_truncated = self._fetch(window)

        if maybe_truncated:
            if window.duration <= MIN_BISECT:
                raise SourceTruncated(
                    f"{self.signal}: {self.row_limit} rows returned for {window}, which is "
                    f"already at the minimum subdivision. Raise row_limit or narrow the query."
                )
            log.debug(
                "%s: %d rows for %s hit the row limit — bisecting",
                self.signal,
                len(records),
                window,
            )
            left, right = window.bisect()
            return self.read(left) + self.read(right)

        kept = [record for record in records if window.contains(record.timestamp)]

        if len(kept) != len(records):
            # Expected and correct for VictoriaMetrics (inclusive end) and Tempo
            # (overlap matching). Logged at debug because seeing it drop to zero would
            # be the first sign a backend changed its convention under us.
            log.debug(
                "%s: dropped %d of %d records outside %s",
                self.signal,
                len(records) - len(kept),
                len(records),
                window,
            )

        self._assert_unique(kept, window)
        return kept

    def _assert_unique(self, records: list[RawRecord], window: TimeRange) -> None:
        """Guard against an identity scheme that cannot actually tell records apart.

        If two distinct source records hash to one identity, deduplication downstream
        silently discards real data; if one record yields two identities, it is counted
        twice. Both are invisible without this check, and both are bugs in *our* code
        rather than the backend's -- so they are worth catching at the point of read.
        """
        counts = Counter(record.identity for record in records)
        collisions = [identity for identity, count in counts.items() if count > 1]
        if collisions:
            raise SourceError(
                f"{self.signal}: {len(collisions)} duplicate record identities in {window} "
                f"(e.g. {collisions[0]}). The identity scheme cannot distinguish these "
                f"records, so they would be double-counted or silently merged."
            )

    def _get(self, path: str, params: dict[str, object]) -> httpx.Response:
        """HTTP GET with backend-attributed errors.

        A bare httpx error names a URL; when three backends are being read in one run,
        the signal name is what actually tells you which one broke.
        """
        url = f"{self.base_url}{path}"
        try:
            response = self.client.get(url, params=params)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise SourceError(
                f"{self.signal}: {url} returned {exc.response.status_code}: "
                f"{exc.response.text[:200]}"
            ) from exc
        except httpx.HTTPError as exc:
            raise SourceError(f"{self.signal}: {url} unreachable: {exc}") from exc
        return response
