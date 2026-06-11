# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""scenario_bindings.py — the per-scenario authoring API for a plugin's conn-test/scenarios.py.

This module doubles as the in-code index of the scenario catalog: one constructor
per scenario, each naming exactly the knobs that scenario accepts.

Each catalog scenario has a constructor here that exposes ONLY the knobs that
scenario accepts, so an invalid combination is a ``TypeError`` at authoring time
rather than a runtime binding defect: ``slow_link(latency_ms=...)`` is valid, but
``round_trip_10_buffers(latency_ms=...)`` simply has no such parameter, and
``round_trip_n_buffers(...)`` without ``n_buffers`` will not call. Outcomes are
typed (``error=<code>`` / ``loss=...``) rather than free strings.

Each constructor returns the internal :class:`discovery.Scenario` the framework
carries — plugins import these constructors, not ``Scenario`` itself. The catalog
(``scenarios.py``) stays the single source of truth for each scenario's intrinsic
properties (buffer count + lock, ``needs_link``, the default latency); a
constructor's per-binding knob defaults to ``None`` so the catalog default wins
unless the binding overrides it.
"""

from __future__ import annotations

from conntest_runner.discovery import Scenario


def _error(code: int | str | None) -> str:
    """Render an OK/ERROR outcome from a typed code (``None`` ⇒ OK)."""
    return "OK" if code is None else f"ERROR {code}"


def _loss(code: int | bool) -> str:  # noqa: FBT001  # `code` is a loss-spec value (True = bare LOSS), not a flag
    """Render a LOSS outcome: bare for a source loss (``code=True``), ``LOSS <code>`` for a sink."""
    return "LOSS" if code is True else f"LOSS {code}"


# ── round-trip tiers (always OK; one or more config globs) ──────────────


def round_trip_1_buffer(*globs: str, setup_file: str | None = None) -> Scenario:
    """Single-buffer happy-path round trip over the given config glob(s)."""
    return Scenario("round_trip_1_buffer", configs=list(globs), setup_file=setup_file)


def round_trip_10_buffers(*globs: str, setup_file: str | None = None) -> Scenario:
    """Ten-buffer happy-path round trip — the default functional tier."""
    return Scenario("round_trip_10_buffers", configs=list(globs), setup_file=setup_file)


def round_trip_100_buffers(*globs: str, setup_file: str | None = None) -> Scenario:
    """Hundred-buffer round trip — the sustained-churn tier."""
    return Scenario("round_trip_100_buffers", configs=list(globs), setup_file=setup_file)


def round_trip_n_buffers(*globs: str, n_buffers: int, setup_file: str | None = None) -> Scenario:
    """Configurable round trip — ``n_buffers`` is required (the tier has no default count)."""
    return Scenario(
        "round_trip_n_buffers", configs=list(globs), n_buffers=n_buffers, setup_file=setup_file
    )


def empty(*globs: str, setup_file: str | None = None) -> Scenario:
    """Lifecycle-only open→close (nothing seeded)."""
    return Scenario("empty", configs=list(globs), setup_file=setup_file)


# ── config validation (BIND-only: no setup_file, no live service) ────


def config_valid(path: str) -> Scenario:
    """BIND config(s) that must bind clean.

    ``path`` is a glob under ``conn-test/configs/``; every matched file must
    BIND without error (OK). BIND-only — it takes no ``setup_file`` (it
    spins up no live service).
    """
    return Scenario("config_valid", config=path)


def config_invalid(path: str, *, error: int | str) -> Scenario:
    """BIND config(s) that must be rejected with the declared error code.

    ``path`` is a glob under ``conn-test/configs/``; every matched file must be
    rejected with ``error`` (a code number or its .inc name). Bind one
    ``config_invalid`` per distinct error code — a glob fans out to many files that
    share the code. BIND-only — it takes no ``setup_file``.
    """
    return Scenario("config_invalid", config=path, outcome=_error(error))


# ── fault scenarios (a single config + a typed outcome) ─────────────────


def bad_endpoint(config: str, *, error: int | str, setup_file: str | None = None) -> Scenario:
    """OPEN/START against a dead endpoint — ``error`` is the required expected code."""
    return Scenario("bad_endpoint", config=config, outcome=_error(error), setup_file=setup_file)


def reconnect(
    config: str,
    *,
    error: int | str | None = None,
    loss: int | bool | None = None,
    setup_file: str | None = None,
) -> Scenario:
    """Warm link cut+restore.

    OK by default; ``error=<code>`` if the connector raises and cannot recover;
    ``loss=...`` if the link heals but the second batch is dropped — e.g. a short
    idle budget that lets the source self-terminate during the gap (``loss=True``
    bare for a connector-OK source loss, ``loss=<code>`` for a sink).
    """
    if error is not None and loss is not None:
        raise ValueError("reconnect: pass error= or loss=, not both")
    outcome = _error(error) if loss is None else _loss(loss)
    return Scenario("reconnect", config=config, outcome=outcome, setup_file=setup_file)


def outage(
    config: str, *, loss: int | bool | None = None, setup_file: str | None = None
) -> Scenario:
    """A batch in flight while the link is cut.

    ``loss=None`` ⇒ it survives (OK); ``loss=True`` ⇒ a connector-OK source loss
    (bare LOSS, the broker dropped it); ``loss=<code>`` ⇒ a sink whose during-cut
    write fails with that error (LOSS ``<code>``).
    """
    outcome = "OK" if loss is None else _loss(loss)
    return Scenario("outage", config=config, outcome=outcome, setup_file=setup_file)


def silent_link(
    config: str,
    *,
    error: int | str | None = None,
    loss: int | bool | None = None,
    step_timeout_ms: int | None = None,
    setup_file: str | None = None,
) -> Scenario:
    """A long (~15 s) silence on a live connection the connector must bridge.

    OK by default; a connector that cannot bridge the gap declares how it fails —
    ``error=<code>`` (it raises on the post-gap work) or ``loss=...`` (the
    connection survives but the second batch is dropped: ``loss=True`` bare, or
    ``loss=<code>``). The same connector is often bound twice: a tolerant config
    bridges it (OK), a strict one — e.g. an idle budget that fires during the
    gap — does not (error/loss).

    ``step_timeout_ms`` raises the per-step watchdog for a config that bridges the
    gap by *waiting* — e.g. a long idle budget that holds the post-gap read open,
    which can outlast the 30 s default.
    """
    if error is not None and loss is not None:
        raise ValueError("silent_link: pass error= or loss=, not both")
    outcome = _error(error) if loss is None else _loss(loss)
    return Scenario(
        "silent_link",
        config=config,
        outcome=outcome,
        step_timeout_ms=step_timeout_ms,
        setup_file=setup_file,
    )


def crash_recovery(
    config: str, *, error: int | str | None = None, setup_file: str | None = None
) -> Scenario:
    """Cold restart over an ungraceful process death.

    OK by default; ``error=<code>`` if the connector cannot resume.
    """
    return Scenario("crash_recovery", config=config, outcome=_error(error), setup_file=setup_file)


def slow_link(
    config: str,
    *,
    latency_ms: int | None = None,
    step_timeout_ms: int | None = None,
    error: int | str | None = None,
    setup_file: str | None = None,
) -> Scenario:
    """Degraded-but-alive link (bidirectional latency).

    ``latency_ms`` overrides the catalog default (``None`` keeps it); OK by
    default, ``error=<code>`` if the connector cannot tolerate the delay.
    ``step_timeout_ms`` raises the per-step watchdog for a connector whose work
    legitimately takes longer under the delay (instead of dialing the latency
    down) — ``None`` keeps the harness default.
    """
    return Scenario(
        "slow_link",
        config=config,
        latency_ms=latency_ms,
        step_timeout_ms=step_timeout_ms,
        outcome=_error(error),
        setup_file=setup_file,
    )
