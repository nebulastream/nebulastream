/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

/// Runs the DummyHttpServer on its own, so a query can be pointed at it by hand. The end-to-end
/// test starts the same server inside its own process and needs none of this; this binary exists so
/// that a reviewer can run the demo operator from a terminal and watch it work.

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <thread>

#include <fmt/format.h>

#include <Http/DummyHttpServer.hpp>

namespace
{
std::atomic<bool> interrupted{false};
static_assert(std::atomic<bool>::is_always_lock_free, "the signal handler below writes this flag");

void onInterrupt(int)
{
    interrupted.store(true, std::memory_order_relaxed);
}
}

int main()
{
    std::signal(SIGINT, onInterrupt);
    std::signal(SIGTERM, onInterrupt);

    NES::DummyHttpServer server;
    server.start();
    fmt::print("dummy http server listening on {}\n", server.url(""));
    fmt::print("routes: /echo /upper /slow?ms=N /fail /flaky?n=N\n");
    std::fflush(stdout);

    /// The server owns its own threads; this one only waits for the interrupt.
    while (!interrupted.load(std::memory_order_relaxed))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    fmt::print("answered {} requests\n", server.requestCount());
    std::fflush(stdout);
    server.stop();
    return 0;
}
