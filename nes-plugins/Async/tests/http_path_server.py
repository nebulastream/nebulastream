#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""HTTP server for the async e2e tests: answers every POST with "<path>:<body>" and logs each path.
A `delay_ms` query parameter delays the answer, which makes the service slower than the queries calling it.
`fail_first` and `fail_after` turn answers into 500s, for the failure and retry tests.

Binds an ephemeral port on 127.0.0.1 and prints it as the first line on stdout. Built on asyncio
rather than http.server: one thread per connection tops out below the rate the stress test needs.
Speaks just enough HTTP/1.1 for the HTTP_CALL client: Content-Length bodies on kept-alive connections.
"""

import asyncio
import sys
from urllib.parse import parse_qs, urlsplit

# Requests answered so far, per path including its query.
requests = {}

async def serve(reader, writer):
    try:
        while True:
            head = await reader.readuntil(b"\r\n\r\n")
            request_line, *header_lines = head.decode().split("\r\n")
            path = request_line.split(" ")[1]
            length = 0
            for line in header_lines:
                name, _, value = line.partition(":")
                if name.strip().lower() == "content-length":
                    length = int(value)
            body = (await reader.readexactly(length)).decode()
            query = parse_qs(urlsplit(path).query)
            await asyncio.sleep(int(query.get("delay_ms", ["0"])[0]) / 1000)
            requests[path] = requests.get(path, 0) + 1
            # fail_first=K answers the first K requests to a path with 500, fail_after=N every request after the first N.
            failing = requests[path] <= int(query.get("fail_first", ["0"])[0]) or requests[path] > int(
                query.get("fail_after", [str(sys.maxsize)])[0]
            )
            status = b"500 Internal Server Error" if failing else b"200 OK"
            response = f"{path}:{body}".encode()
            writer.write(
                b"HTTP/1.1 " + status + b"\r\nContent-Type: text/plain\r\nContent-Length: "
                + str(len(response)).encode()
                + b"\r\n\r\n"
                + response
            )
            await writer.drain()
            print(path, file=sys.stderr, flush=True)
    except (asyncio.IncompleteReadError, ConnectionError):
        pass
    finally:
        writer.close()


async def main():
    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    print(server.sockets[0].getsockname()[1], flush=True)
    await server.serve_forever()


asyncio.run(main())
