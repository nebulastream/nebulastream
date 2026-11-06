# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for endpoint parsing, which every loader shares."""

from __future__ import annotations

import pytest

from conntest_runner.endpoint import split_endpoint


@pytest.mark.parametrize(
    ("endpoint", "expected"),
    [
        ("relay:9000", ("relay", 9000)),
        # The last colon separates the port, so a colon-bearing host (an IPv6 literal) stays whole.
        ("a:b:1234", ("a:b", 1234)),
    ],
)
def test_splits_host_and_int_port(endpoint: str, expected: tuple[str, int]) -> None:
    assert split_endpoint(endpoint) == expected


@pytest.mark.parametrize("endpoint", ["nohostport", ":1883", "host:", "host:notaport"])
def test_malformed_endpoint_raises(endpoint: str) -> None:
    with pytest.raises(ValueError, match="host:port|integer"):
        split_endpoint(endpoint)
