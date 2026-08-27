"""Phase 0 tests: configuration loads, and the CLI surface is real.

Thin by design. The point of a Phase 0 suite is to prove the package imports, the
entrypoint parses, and the values that the whole pipeline's correctness rests on --
the per-signal grace periods -- are actually wired to the environment rather than
hardcoded somewhere they cannot be tuned.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from edgerollup.cli import _iso, build_parser
from edgerollup.config import DEFAULT_CONFIG_PATH, SIGNALS, Settings, load_rollup_config


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> Settings:
    """Settings built from defaults only.

    A developer's real .env would otherwise leak into these assertions, which is how a
    suite ends up passing on one machine and failing in CI.
    """
    for signal in SIGNALS:
        monkeypatch.delenv(f"GRACE_{signal.upper()}_SECONDS", raising=False)
    return Settings(_env_file=None)


class TestGrace:
    def test_defaults_match_the_documented_boundary_policy(self, settings: Settings):
        # These are not arbitrary numbers -- each covers every buffer between emission
        # and queryability for its backend (decisions.md D-001). If one changes, the
        # reasoning in decisions.md must change with it.
        assert settings.grace("metrics") == timedelta(minutes=2)
        assert settings.grace("logs") == timedelta(minutes=10)
        assert settings.grace("traces") == timedelta(minutes=20)

    def test_grace_increases_with_backend_buffering(self, settings: Settings):
        """The ordering is the invariant, more than the exact values.

        Metrics clear fastest, logs must survive Loki's chunk lifecycle, traces must
        additionally survive Tempo's block lifecycle. A change that inverted this would
        mean someone had stopped reasoning about the backends and started tuning numbers.
        """
        assert settings.grace("metrics") < settings.grace("logs") < settings.grace("traces")

    def test_unknown_signal_raises_rather_than_defaulting(self, settings: Settings):
        # A typo must not silently inherit the shortest grace in the system: that would
        # roll up unsealed windows and lose data quietly, which is the single worst
        # failure this pipeline can have.
        with pytest.raises(ValueError, match="unknown signal"):
            settings.grace("metric")

    def test_grace_is_environment_tunable(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("GRACE_TRACES_SECONDS", "1800")
        assert Settings(_env_file=None).grace("traces") == timedelta(minutes=30)


class TestRollupConfig:
    def test_shipped_config_loads(self):
        config = load_rollup_config(DEFAULT_CONFIG_PATH)
        assert config["schema_version"] == 1
        assert set(config["signals"]) == set(SIGNALS)

    def test_service_instance_id_is_not_a_rollup_dimension(self):
        """Guards decisions.md D-005.

        service_instance_id is a UUID regenerated on every app restart. As a rollup
        dimension it would grow cold-tier cardinality without bound -- the exact failure
        the upstream project exists to prevent, reproduced in the tier that is supposed
        to be cheap. This test exists so re-adding it is a deliberate act.
        """
        config = load_rollup_config(DEFAULT_CONFIG_PATH)
        every_dimension = set(config["common_dimensions"])
        for signal in config["signals"].values():
            every_dimension.update(signal.get("dimensions", []))

        # Non-emptiness first (D-022): with no dimensions configured at all, the two
        # assertions below would pass while proving nothing.
        assert len(every_dimension) >= 5, f"suspiciously few dimensions: {every_dimension}"
        assert "service_instance_id" not in every_dimension
        assert "run_id" not in every_dimension

    def test_traces_are_never_written_back_to_tempo(self):
        """Tempo has no host write path, and rolled-up traces are not traces anyway."""
        config = load_rollup_config(DEFAULT_CONFIG_PATH)
        assert "tempo" not in config["signals"]["traces"]["sinks"]

    def test_missing_config_raises_a_useful_error(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_rollup_config(tmp_path / "nope.yaml")


class TestCli:
    def test_parser_builds_and_declares_every_verb(self):
        parser = build_parser()
        # Parsing --help raises SystemExit(0); that it does so proves the parser is
        # well-formed, which is the Phase 0 gate.
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["--help"])
        assert exc.value.code == 0

    @pytest.mark.parametrize("verb", ["run", "backfill", "probe", "status"])
    def test_verbs_are_registered(self, verb: str):
        from edgerollup.cli import HANDLERS

        assert verb in HANDLERS

    def test_naive_timestamps_are_assumed_utc(self):
        """Local time is the wrong default for a job whose correctness is bucket-aligned.

        On a machine in a DST zone, interpreting naive input as local would silently
        produce a duplicated or a missing hour twice a year.
        """
        parsed = _iso("2026-08-27T10:00:00")
        assert parsed.utcoffset() == timedelta(0)

    def test_offset_timestamps_are_normalised_to_utc(self):
        parsed = _iso("2026-08-27T10:00:00+05:30")
        assert parsed.utcoffset() == timedelta(0)
        assert parsed.hour == 4 and parsed.minute == 30
