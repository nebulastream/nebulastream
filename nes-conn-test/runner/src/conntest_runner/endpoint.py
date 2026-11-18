# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Parses the ``host:port`` endpoint that the framework passes to a loader, so all loaders validate it the same way."""

from __future__ import annotations


def split_endpoint(endpoint: str) -> tuple[str, int]:
    host, sep, port = endpoint.rpartition(":")
    if not sep or not host or not port:
        raise ValueError(f"endpoint must be 'host:port', got {endpoint!r}")
    try:
        return host, int(port)
    except ValueError:
        raise ValueError(f"endpoint port must be an integer, got {endpoint!r}") from None
