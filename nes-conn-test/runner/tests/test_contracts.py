# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Framework self-test for conntest_runner.contracts — the loader ABC guardrails.

The loader base classes are the framework's main extension seam, so the point of
these tests is the *feedback a connector developer gets when they get it wrong*: an
unimplemented contract method must fail loudly and name the missing piece, not surface
as a confusing runtime error deep inside a Docker battery run. Each test below pins one
failure mode and asserts the message points at the fix.
"""

from __future__ import annotations

import pytest

from conntest_runner.contracts import SinkLoader, SourceLoader


def test_source_loader_missing_method_cannot_be_instantiated() -> None:
    # seed_batch is left abstract: the class stays abstract and instantiation fails,
    # naming the method the developer still owes. Discovery instantiates the loader,
    # so this is the error a connector author would actually hit.
    class _Incomplete(SourceLoader):
        def config_overrides(self, *, target, endpoint):  # noqa: ARG002
            return {}

        def setup(self, *, target, schema=None): ...

    with pytest.raises(TypeError, match="seed_batch"):
        _Incomplete()


def test_sink_loader_missing_method_cannot_be_instantiated() -> None:
    class _Incomplete(SinkLoader):
        def config_overrides(self, *, target, endpoint):  # noqa: ARG002
            return {}

        def setup(self, *, target, schema=None): ...

    with pytest.raises(TypeError, match="read_back"):
        _Incomplete()


def test_loader_missing_config_overrides_cannot_be_instantiated() -> None:
    # config_overrides lives on the common Loader base; a loader that forgets it
    # is still abstract.
    class _Incomplete(SourceLoader):
        def setup(self, *, target, schema=None): ...

        def seed_batch(self, rows, *, target): ...

    with pytest.raises(TypeError, match="config_overrides"):
        _Incomplete()


def test_complete_source_loader_constructs() -> None:
    # Positive control: a loader that satisfies the whole contract defines and
    # constructs cleanly (no false positives from the guards above).
    class _Good(SourceLoader):
        def config_overrides(self, *, target, endpoint):  # noqa: ARG002
            return {}

        def setup(self, *, target, schema=None): ...

        def seed_batch(self, rows, *, target): ...

    assert _Good() is not None


def test_complete_sink_loader_constructs() -> None:
    class _Good(SinkLoader):
        def config_overrides(self, *, target, endpoint):  # noqa: ARG002
            return {}

        def setup(self, *, target, schema=None): ...

        def read_back(self, *, target, schema, expect_records):  # noqa: ARG002
            return []

    assert _Good() is not None
