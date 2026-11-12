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

#include <HttpCallAsyncFunction.hpp>
#include <HttpCallResolver.hpp>

#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <Util/CheckedDeadline.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/http.hpp>
#include <boost/system/system_error.hpp>
#include <boost/url/parse.hpp>
#include <AsyncFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
namespace http = boost::beast::http;

/// One budget for DNS resolution, connect, request write, and response read, per attempt.
constexpr std::chrono::milliseconds DEFAULT_TIMEOUT = std::chrono::seconds(30);
}

AsyncFunctionRegistryReturnType HttpCallAsyncFunction::create(AsyncFunctionRegistryArguments)
{
    return std::make_unique<HttpCallAsyncFunction>();
}

std::optional<size_t> HttpCallAsyncFunction::maxRetries(const AsyncValues& arguments) const
{
    PRECONDITION(arguments.size() >= 2 && arguments.size() <= 4, "HTTP_CALL takes (body, url[, timeout_ms[, retries]])");
    if (arguments.size() < 4)
    {
        return size_t{0};
    }
    const auto retries = arguments[3].getUnsignedInteger();
    if (!std::in_range<size_t>(retries) || retries == std::numeric_limits<size_t>::max())
    {
        throw CannotReadFromSource("HTTP_CALL expects a representable retry count, got {}", retries);
    }
    return static_cast<size_t>(retries);
}

boost::asio::awaitable<AsyncValues> HttpCallAsyncFunction::call(AsyncValues arguments)
{
    PRECONDITION(arguments.size() >= 2 && arguments.size() <= 4, "HTTP_CALL takes (body, url[, timeout_ms[, retries]])");
    const auto startedAt = std::chrono::steady_clock::now();
    const std::string body{arguments[0].get<std::string_view>()};
    const std::string url{arguments[1].get<std::string_view>()};
    auto millis = static_cast<uint64_t>(DEFAULT_TIMEOUT.count());
    if (arguments.size() >= 3)
    {
        millis = arguments[2].getUnsignedInteger();
        if (millis == 0)
        {
            throw CannotReadFromSource("HTTP_CALL expects a positive timeout in milliseconds, got {}", millis);
        }
    }
    const auto deadline = checkedSteadyDeadline(millis, "HTTP_CALL", startedAt);
    const auto parsed = boost::urls::parse_uri(url);
    if (!parsed || parsed->scheme() != "http")
    {
        throw CannotReadFromSource("HTTP_CALL needs an http:// url, got {}", url);
    }
    const std::string host = parsed->host();
    const std::string port = parsed->has_port() ? std::string{parsed->port()} : "80";
    const std::string target = parsed->encoded_target().empty() ? "/" : std::string{parsed->encoded_target()};

    /// A transport error may follow a successfully processed POST. Only the explicit retry policy may resend it.
    try
    {
        co_return AsyncValues::make(co_await exchange(host, port, target, body, deadline));
    }
    catch (const boost::system::system_error& error)
    {
        stream.reset();
        throw CannotReadFromSource("HTTP_CALL to {} failed: {}", url, error.what());
    }
}

/// The analyzer follows Asio's awaitable frames into paths that the coroutine machinery never takes.
/// NOLINTBEGIN(clang-analyzer-core.CallAndMessage)
/// NOLINTNEXTLINE(performance-unnecessary-value-param) a coroutine has to own what it reads after its first suspension
boost::asio::awaitable<std::string> HttpCallAsyncFunction::exchange(
    std::string host, std::string port, std::string target, std::string body, const std::chrono::steady_clock::time_point deadline)
{
    using boost::asio::use_awaitable;
    const auto requireTimeRemaining = [deadline]
    {
        if (std::chrono::steady_clock::now() >= deadline)
        {
            throw boost::system::system_error{boost::asio::error::timed_out};
        }
    };
    requireTimeRemaining();
    if (!stream || connectedTo != host + ":" + port)
    {
        const auto executor = co_await boost::asio::this_coro::executor;
        const auto endpoints = co_await detail::resolveHttpHost(host, port, deadline);
        requireTimeRemaining();
        stream.emplace(executor);
        buffer.clear();
        stream->expires_at(deadline);
        co_await stream->async_connect(endpoints, use_awaitable);
        connectedTo = host + ":" + port;
    }

    http::request<http::string_body> request{http::verb::post, target, 11, std::move(body)};
    request.set(http::field::host, host + ":" + port);
    request.set(http::field::content_type, "text/plain");
    request.keep_alive(true);
    request.prepare_payload();
    requireTimeRemaining();
    stream->expires_at(deadline);
    co_await http::async_write(*stream, request, use_awaitable);

    http::response<http::string_body> response;
    requireTimeRemaining();
    stream->expires_at(deadline);
    co_await http::async_read(*stream, buffer, response, use_awaitable);
    requireTimeRemaining();
    if (!response.keep_alive())
    {
        stream.reset();
    }
    if (response.result_int() < 200 || response.result_int() >= 300)
    {
        throw CannotReadFromSource("HTTP_CALL to {}:{}{} got status {}: {}", host, port, target, response.result_int(), response.body());
    }
    co_return std::move(response.body());
}

/// NOLINTEND(clang-analyzer-core.CallAndMessage)

}
