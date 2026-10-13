# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""compose.py — the ``service:port`` endpoint that the conn-test runner hands to the harness.

The conn-test runner now lives inside the framework ``runner`` service
(``nes-conn-test/runner/runner.compose.yaml``), merged on top of the
per-plugin data-service compose file and brought up / torn down by the
``conntest`` CLI (``conntest_runner.cli``) outside pytest. From the
runner's perspective the broker is just another service on the same
compose network, addressable by service name via the embedded resolver.
There's no host-side discovery, no host-port mapping, and no
self-container-ID dance — the endpoint is exactly ``<service>:<port>``
as declared in the plugin's loader.

This module used to wrap ``docker compose up/down`` and resolve
``host.docker.internal`` for the dev container; that whole layer is
gone now that the runner sits inside the compose network. All that's
left is the lightweight ``ComposeEndpoint`` value type so existing
imports keep working.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ComposeEndpoint:
    """The compose-internal endpoint of a service-under-test.

    Always ``service:container_port``. The runner is on the same
    compose network as the service, so this resolves directly via the
    docker embedded DNS on 127.0.0.11.
    """

    host: str
    port: int

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"
