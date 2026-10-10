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

#include <chrono>
#include <cstddef>
#include <expected>
#include <functional>
#include <memory>

#include <Http/HttpBackend.hpp>

namespace NES
{

/// What a finished request produced: a response, or the reason there was none. Called once per
/// request, retries included.
using HttpCompletion = std::move_only_function<void(std::expected<HttpResponse, HttpError>)>;

/// Many requests in flight on one or two threads.
///
/// `CurlHttpBackend` needs a thread per request in flight, because `send` blocks until the answer
/// arrives. An operator with 256 concurrent calls therefore needs 256 threads that do nothing but
/// wait, which is what this exists to avoid: the same libcurl runs on a Boost.Asio event loop
/// through curl's multi interface, which hands its sockets and its timeouts to the loop. The number
/// of requests in flight then has nothing to do with the number of threads.
///
/// Asio for the loop and libcurl for the HTTP, rather than Boost.Beast: libcurl already does TLS,
/// redirects, connection reuse and the timeout handling that the blocking client is measured with,
/// and Beast would be a new dependency — which changes the dependency hash, and with it the
/// prebuilt development image.
///
/// One loop per asynchronous operator is what the framework creates, which keeps the lifetime
/// trivial: nothing outlives the operator. Sharing one between operators works all the same —
/// `send` and `after` are safe to call from any thread, and the only reason not to share is that a
/// loop's threads also run the completions, so one operator's decoding would delay another's.
class HttpEventLoop
{
public:
    /// `threads` run the loop. One is enough to keep any number of requests in flight; a second
    /// helps only when the completions themselves cost something, such as parsing a large answer.
    explicit HttpEventLoop(size_t threads = 1);
    ~HttpEventLoop();

    HttpEventLoop(const HttpEventLoop&) = delete;
    HttpEventLoop& operator=(const HttpEventLoop&) = delete;
    HttpEventLoop(HttpEventLoop&&) = delete;
    HttpEventLoop& operator=(HttpEventLoop&&) = delete;

    /// Starts `request` and calls `done` on one of this loop's threads once it is finished.
    /// Returns immediately. Retries happen inside, on a timer, so `done` runs exactly once and
    /// carries the final outcome. Calling after `stop` invokes `done` with an error instead.
    void send(HttpRequest request, HttpCompletion done);

    /// Runs `work` on one of this loop's threads after `delay`. It is what the retry backoff uses,
    /// and what an operator needs in order to wait without occupying a thread.
    void after(std::chrono::milliseconds delay, std::move_only_function<void()> work);

    /// Abandons everything in flight — each pending completion is called with an error — and joins
    /// the threads. No completion can run once this has returned, which is what makes it safe to
    /// destroy whatever the completions captured. Idempotent; the destructor calls it.
    void stop();

    /// Requests started and not yet completed. For tests and for diagnostics.
    [[nodiscard]] size_t inFlight() const;

private:
    struct State;
    std::unique_ptr<State> state;
};

}
