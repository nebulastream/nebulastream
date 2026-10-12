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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace NES
{

/// A small HTTP server that answers from the request body alone, so a test can assert on exact
/// output. It exists so that the asynchronous framework can be demonstrated end to end without an
/// LLM, a model server or any other external service.
///
/// Routes, each answering with a plain text body:
///
///   POST /echo           the request body, verbatim
///   POST /upper          the request body, upper-cased
///   POST /slow?ms=N      like /echo, after waiting N ms -- what concurrency is measured with
///   POST /fail           HTTP 500 every time
///   POST /flaky?n=N      HTTP 503 for the first N requests, then /echo -- exercises the retry policy
///   POST /status?code=N  that status, with the request body
///   POST /stall?ms=N     reads the request and never answers, so the client hits its own timeout
///
/// Any other path answers 404. GET behaves the same way, with an empty body.
///
/// It binds an ephemeral port on the loopback interface, so several tests can run at once, and it
/// serves each connection on its own thread, so a slow route does not serialize the others -- a
/// server that handled one request at a time would make any concurrency measurement meaningless.
class DummyHttpServer
{
public:
    DummyHttpServer();
    ~DummyHttpServer();

    DummyHttpServer(const DummyHttpServer&) = delete;
    DummyHttpServer& operator=(const DummyHttpServer&) = delete;
    DummyHttpServer(DummyHttpServer&&) = delete;
    DummyHttpServer& operator=(DummyHttpServer&&) = delete;

    /// Binds and starts serving. Returns once the port is open, so a caller may hand out the URL
    /// immediately.
    void start();

    /// Stops accepting, finishes the connections in flight and joins every thread.
    void stop();

    [[nodiscard]] uint16_t port() const;

    /// `http://127.0.0.1:<port><path>`.
    [[nodiscard]] std::string url(const std::string& path) const;

    /// How many requests have been answered. Lets a test assert that the operator really made the
    /// calls rather than inventing the answers.
    [[nodiscard]] size_t requestCount() const;

private:
    struct State;
    std::unique_ptr<State> state;
};

}
