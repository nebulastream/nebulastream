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
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace NES
{

enum class HttpMethod : uint8_t
{
    GET,
    POST,
};

/// One HTTP exchange, fully described by its caller. The transport assembles nothing: no base URL,
/// no content type, no authentication scheme. Whatever protocol is spoken over this -- chat
/// completions, a JSON API, plain text -- lives in the operator's own module.
struct HttpRequest
{
    std::string url;
    HttpMethod method = HttpMethod::POST;
    std::vector<std::pair<std::string, std::string>> headers;
    /// Ignored for GET.
    std::string body;
    /// Upper bound for one attempt, connect included (libcurl's CURLOPT_TIMEOUT).
    std::chrono::seconds timeout{600};
    /// The one home of this default. libcurl's own is "wait forever".
    std::chrono::milliseconds connectTimeout{10000};
    /// Additional attempts after the first on a transport failure, HTTP 429 or 5xx.
    size_t maxRetries = 2;
};

struct HttpResponse
{
    /// Whatever the endpoint answered. A non-2xx status is still an answer; interpreting it is the
    /// caller's business, because what counts as a failure depends on the protocol.
    long status = 0;
    std::string body;
};

/// No answer at all, after every retry: refused, DNS, TLS, timeout. A completed exchange is never
/// an error here, which is why this carries no status and no kind.
struct HttpError
{
    std::string message;
};

/// Transport for operators that call an external service over HTTP.
///
/// Implementations are not thread-safe -- a connection is a piece of mutable state. The caller
/// keeps one instance per thread that sends; `HttpOperatorExecutor` in nes-async does that for
/// asynchronous operators, and a synchronous operator would keep one per worker thread.
class HttpBackend
{
public:
    virtual ~HttpBackend() = default;
    [[nodiscard]] virtual std::expected<HttpResponse, HttpError> send(const HttpRequest& request) = 0;
};

/// Creates one backend instance. Exists so that a caller can be handed a fake in a test without
/// knowing which implementation it would otherwise get.
using HttpBackendFactory = std::function<std::unique_ptr<HttpBackend>()>;

/// A factory for the libcurl implementation. Declared here so that nothing but the one
/// implementation file has to include CurlHttpBackend.hpp.
[[nodiscard]] HttpBackendFactory defaultHttpBackendFactory();

}
