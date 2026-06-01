# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""link.py — a toxiproxy-backed connection the runner can sever and heal.

A `Link` fronts the real backend with a toxiproxy proxy: the connector's
config is pointed at ``toxiproxy:<listen_port>`` (which forwards to
``<upstream>``), and ``cut()`` / ``restore()`` disable / re-enable the
proxy to drop and heal the connection deterministically. Control is plain
HTTP/JSON over the stdlib (no new pip dep), at ``toxiproxy:8474`` by
compose DNS — the same trust model as the backend service itself.
"""

from __future__ import annotations

import contextlib
import json
import time
import urllib.error
import urllib.request

_CONTROL = "toxiproxy:8474"
_READY_TIMEOUT_S = 15.0


class Link:
    """A toxiproxy-backed connection the runner can sever (`cut`) and heal."""

    def __init__(
        self,
        *,
        name: str,
        listen_port: int,
        upstream: str,
        control: str = _CONTROL,
    ) -> None:
        self._base = f"http://{control}"
        self._name = name
        self._listen = f"0.0.0.0:{listen_port}"
        self._upstream = upstream

    # -- HTTP helpers ---------------------------------------------------
    def _request(self, method: str, path: str, body: dict[str, str | bool] | None = None) -> bytes:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(  # noqa: S310  # controlled localhost toxiproxy admin URL
            self._base + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310  # controlled localhost toxiproxy admin URL
            raw: bytes = resp.read()
            return raw

    def _wait_ready(self) -> None:
        deadline = time.monotonic() + _READY_TIMEOUT_S
        last: Exception | None = None
        while time.monotonic() < deadline:
            try:
                self._request("GET", "/version")
            except (urllib.error.URLError, OSError) as e:
                last = e
                time.sleep(0.1)
            else:
                return
        raise TimeoutError(f"toxiproxy control API not ready: {last}")

    # -- lifecycle ------------------------------------------------------
    def create(self) -> Link:
        """Create (or reset) the proxy enabled.

        Idempotent across reruns: a stale proxy of the same name is deleted
        first.
        """
        self._wait_ready()
        with contextlib.suppress(urllib.error.HTTPError):
            self._request("DELETE", f"/proxies/{self._name}")  # not present yet — fine
        self._request(
            "POST",
            "/proxies",
            {
                "name": self._name,
                "listen": self._listen,
                "upstream": self._upstream,
                "enabled": True,
            },
        )
        return self

    def cut(self) -> None:
        """Disable the proxy: drop live connections and refuse reconnects."""
        self._request("POST", f"/proxies/{self._name}", {"enabled": False})

    def restore(self) -> None:
        """Re-enable the proxy: the listener is back and forwarding resumes."""
        self._request("POST", f"/proxies/{self._name}", {"enabled": True})

    def cleanup(self) -> None:
        """Best-effort delete of the proxy; ignore if the control API is gone."""
        with contextlib.suppress(urllib.error.URLError, OSError):
            self._request("DELETE", f"/proxies/{self._name}")
