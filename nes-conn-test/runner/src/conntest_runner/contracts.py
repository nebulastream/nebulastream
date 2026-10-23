# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The loader contract.
A loader is the only code that a connector adds to the framework, and it does only backend IO.

Loaders subclass these bases, so a missing method fails at construction, and discovery finds a loader by its base class.
``rows`` and ``schema`` are typed ``Any``, so each loader can declare precise types without a contravariance clash.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Loader(ABC):
    def __init__(self, endpoint: str | None) -> None:
        """``endpoint`` is the compose service's address, such as ``broker:1883``.

        A connector without a compose service gets None, and its loader sets ``endpoint`` to a server that it starts itself.
        """
        self.endpoint = endpoint

    @abstractmethod
    def config_overrides(self, *, target: str, endpoint: str) -> dict[str, str]:
        """Maps the per-test ``endpoint`` and ``target`` onto the keys that the ``.nesql`` declares."""

    @abstractmethod
    def setup(self, *, target: str, schema: Any) -> None:
        """Prepares the backend before the connector opens.

        It is always called, so a loader with nothing to prepare implements it empty.
        """


class SourceLoader(Loader):
    @abstractmethod
    def seed_batch(self, rows: Any, *, target: str) -> None:
        """A connector whose wire format is JSON serializes the rows with the framework's JSONL codec, not its own."""

    def wait_seeded(self) -> None:  # noqa: B027  # an optional hook, not an abstract method
        """Returns once every seeded batch has reached the backend.

        A scenario that checks that the source holds data back calls it, so that the check cannot pass only because the data was still in flight.
        A loader whose ``seed_batch`` returns only after the backend has the rows keeps this default.
        """
