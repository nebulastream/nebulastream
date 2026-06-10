# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""contracts.py — the connector-loader contract as abstract base classes.

A loader is the one place every connector extends the framework: it does the
backend IO a scenario drives (seed a source, read a sink back) while topology,
config, resource naming, and the data model stay framework-owned (see
core_design_principles.md). That seam used to be an informal, duck-typed
contract — ``getattr`` probes and prose docstrings — so a typo in a plugin's
``read_back`` signature or a forgotten ``config_overrides`` only surfaced at
runtime, inside Docker, despite the package running ``mypy --strict``.

A loader now *inherits* ``SourceLoader`` / ``SinkLoader``, and that is the single
source of truth for what a loader is — there is no parallel duck-typed path:
construction fails (``TypeError``) if an ``@abstractmethod`` is left unimplemented,
and mypy checks the override signatures at the class definition, so a wrong
``seed_batch``/``read_back`` signature fails the per-plugin type gate (``check.sh``),
not a battery run.

Discovery resolves a loader by ``issubclass`` against these bases
(``discovery._is_loader_class``), so the base class is the *only* abstraction a
plugin author works against — there is no separate "looks like a loader" rule to
also satisfy.

This does not weaken ``plugin-isolation``: inheriting a base class adds no edit to
any framework file when a connector is added.

``setup`` is part of the contract (not optional): every loader implements it, even
a byte source like MQTT that has no backend to prepare — it implements an empty
``setup`` with a comment saying so. ``scenarios.Ctx.setup`` therefore calls it
unconditionally; there is no capability probe.

The framework deals with loaders in **typed rows** on both ends — a source's
``seed_batch`` receives the generated rows, a sink's ``read_back`` returns the rows
it read. The wire form (JSONL) stays the framework's: the data model generates it and
the C++ harness decodes it, and a connector whose wire is JSON serializes/parses with
the framework's ``rows_to_jsonl`` / ``jsonl_to_rows`` rather than hand-rolling a format.
``rows``/``schema`` are typed ``Any`` because they cross the framework↔loader boundary
where each connector keeps its own precise signature without a contravariance clash.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Loader(ABC):
    """The surface common to every connector loader, regardless of kind.

    ``config_overrides`` rewrites the framework's dynamic config keys (the
    per-test endpoint and resource name) onto the keys the ``.nesql`` declares
    (``config-fidelity``). ``setup`` is the pre-connect backend hook (create the
    per-test table, open a persistent subscriber); a loader with nothing to
    prepare implements it empty.
    """

    @abstractmethod
    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str]:
        """Map the framework's per-test ``endpoint`` and ``target`` onto config keys."""

    @abstractmethod
    def setup(self, *, target: str, schema: Any) -> None:
        """Prepare the backend before the connector opens (implemented empty if nothing to do)."""


class SourceLoader(Loader):
    """A source loader: seeds the backend with the framework's generated rows.

    ``seed_batch`` writes one batch of typed rows to ``target`` however the backend
    needs them — INSERT them, or serialize them to the connector's JSON wire (via the
    framework's ``rows_to_jsonl``) and publish. A loader may accept extra optional
    keywords (e.g. MQTT's ``qos=`` for the outage scenario) on top of this contract.
    """

    @abstractmethod
    def seed_batch(self, rows: Any, *, target: str) -> None:
        """Write one batch of the framework's generated typed rows to ``target``."""


class SinkLoader(Loader):
    """A sink loader: reads the backend back as typed rows.

    ``read_back`` returns the rows the sink wrote as typed tuples — a NATIVE sink
    SELECTs them; an opaque sink parses the records it collected with the framework's
    ``jsonl_to_rows`` (keyed by ``schema``) before returning. The framework canonicalizes
    and multiset-compares them against the oracle, so the return is ``list[Any]``.
    """

    @abstractmethod
    def read_back(self, *, target: str, schema: Any, expect_records: int) -> list[Any]:
        """Return the rows the sink wrote to ``target`` as typed tuples."""
