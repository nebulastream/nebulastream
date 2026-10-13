# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Framework self-test for conntest_runner.endpoint.split_endpoint.

Endpoint parsing is framework policy shared by every loader, so its contract —
``"host:port" -> (str, int)`` and the errors a bad endpoint produces — is pinned
here once instead of in each connector.
"""

from __future__ import annotations

import pytest

from conntest_runner.endpoint import split_endpoint


def test_splits_host_and_int_port() -> None:
    assert split_endpoint("broker:1883") == ("broker", 1883)
    assert split_endpoint("toxiproxy:5432") == ("toxiproxy", 5432)


def test_splits_on_rightmost_colon() -> None:
    # rpartition: the last colon is the port separator, so a colon-bearing host
    # (an IPv6 literal, a scheme) keeps everything left of it.
    assert split_endpoint("a:b:1234") == ("a:b", 1234)


def test_missing_colon_raises() -> None:
    with pytest.raises(ValueError, match="host:port"):
        split_endpoint("nohostport")


def test_empty_host_raises() -> None:
    with pytest.raises(ValueError, match="host:port"):
        split_endpoint(":1883")


def test_empty_port_raises() -> None:
    with pytest.raises(ValueError, match="host:port"):
        split_endpoint("host:")


def test_non_integer_port_raises() -> None:
    with pytest.raises(ValueError, match="integer"):
        split_endpoint("host:notaport")
