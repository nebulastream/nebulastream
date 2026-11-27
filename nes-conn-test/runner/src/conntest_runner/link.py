# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A connection between the connector and its backend that a fault scenario can sever, slow down or silence.

The connector's config points at the proxy, ``toxiproxy:<port>`` with a port that toxiproxy chooses or that the caller sets,
and toxiproxy forwards to the backend.
The loader seeds the backend directly, so a fault affects only the connector's side.
toxiproxy is controlled over HTTP at ``toxiproxy:8474`` by compose DNS, with the stdlib only.
"""

from __future__ import annotations

import contextlib
import json
import time
import urllib.error
import urllib.request
from http import HTTPStatus

from conntest_runner.endpoint import split_endpoint

_CONTROL = "toxiproxy:8474"
_READY_TIMEOUT_S = 15.0
_REQUEST_TIMEOUT_S = 10.0
# silence() delays every byte by this much, which must exceed every silence that a scenario holds,
# so that nothing sent during the silence arrives before unsilence().
_SILENCE_LATENCY_MS = 60_000
_DIRECTIONS = ("upstream", "downstream")


class Link:
    def __init__(self, *, name: str, upstream: str, listen_port: int = 0, control: str = _CONTROL) -> None:
        self._base = f"http://{control}"
        self._host, _ = split_endpoint(control)
        self._name = name
        self._listen = f"0.0.0.0:{listen_port}"
        self._upstream = upstream
        self._port: int | None = None

    @property
    def endpoint(self) -> str:
        """The proxy's address, which the connector's config points at."""
        if self._port is None:
            raise RuntimeError(f"the proxy {self._name!r} has no port before create()")
        return f"{self._host}:{self._port}"

    def _request(self, method: str, path: str, body: dict[str, object] | None = None) -> bytes:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(  # noqa: S310  # the URL is the compose network's toxiproxy
            self._base + path, data=data, method=method, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT_S) as reply:  # noqa: S310  # see above
            return reply.read()

    def _wait_ready(self) -> None:
        """The toxiproxy image has no shell for a compose healthcheck, so readiness is polled here."""
        deadline = time.monotonic() + _READY_TIMEOUT_S
        while True:
            try:
                self._request("GET", "/version")
            except (urllib.error.URLError, OSError) as e:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"the toxiproxy control API did not answer within {_READY_TIMEOUT_S} s: {e}") from e
                time.sleep(0.1)
            else:
                return

    def create(self) -> Link:
        """A proxy of the same name left by an earlier run is replaced.

        Without a listen port, toxiproxy chooses the proxy's port, because a fixed one can be in use in the toxiproxy container.
        A backend's port is an example: a loader that listens on an ephemeral port shares that range with toxiproxy's own connections.
        """
        self._wait_ready()
        try:
            self._request("DELETE", f"/proxies/{self._name}")
        except urllib.error.HTTPError as e:
            # The error holds the response open until it is closed.
            e.close()
            if e.code != HTTPStatus.NOT_FOUND:
                raise
        reply = self._request(
            "POST", "/proxies", {"name": self._name, "listen": self._listen, "upstream": self._upstream, "enabled": True}
        )
        # The reply holds the address that the proxy listens on, such as `[::]:42267`.
        # A cut and a restore keep this port, so a connector that reconnects finds the proxy again.
        _, self._port = split_endpoint(json.loads(reply)["listen"])
        return self

    def cut(self) -> None:
        """Closes the live connections and refuses new ones until restore()."""
        self._request("POST", f"/proxies/{self._name}", {"enabled": False})

    def restore(self) -> None:
        self._request("POST", f"/proxies/{self._name}", {"enabled": True})

    def _add_latency(self, prefix: str, latency_ms: int) -> None:
        # Both directions, so the scenario does not depend on which way the connector's data flows.
        for stream in _DIRECTIONS:
            self._request(
                "POST",
                f"/proxies/{self._name}/toxics",
                {
                    "name": f"{prefix}_{stream}",
                    "type": "latency",
                    "stream": stream,
                    "attributes": {"latency": latency_ms, "jitter": 0},
                },
            )

    def throttle(self, *, latency_ms: int) -> None:
        """Delays every byte by ``latency_ms`` in each direction; there is no jitter, so runs are repeatable."""
        self._add_latency("latency", latency_ms)

    def silence(self) -> None:
        """No byte flows, but the connection stays established: no FIN and no RST.

        A long latency is used instead of the ``timeout`` toxic with ``timeout=0``,
        because removing that toxic closes the stalled connection, which would turn the silence into a cut.
        """
        self._add_latency("silence", _SILENCE_LATENCY_MS)

    def unsilence(self) -> None:
        for stream in _DIRECTIONS:
            self._request("DELETE", f"/proxies/{self._name}/toxics/silence_{stream}")

    def cleanup(self) -> None:
        with contextlib.suppress(urllib.error.URLError, OSError):
            self._request("DELETE", f"/proxies/{self._name}")
