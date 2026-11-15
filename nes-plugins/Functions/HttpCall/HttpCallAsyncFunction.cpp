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

#include <DataTypes/CheckedInteger.hpp>
#include <HttpCallAsyncFunction.hpp>
#include <HttpCallResolver.hpp>

#include <cerrno>
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

namespace
{
size_t parseRetries(const SharedRuntimeValues& arguments)
{
    PRECONDITION(arguments.size() >= 2 && arguments.size() <= 4, "HTTP_CALL takes (body, url[, timeout_ms[, retries]])");
    if (arguments.size() < 4)
    {
        return size_t{0};
    }
    const auto retries = checkedUnsignedIntegerOption(arguments[3], "HTTP_CALL retries");
    if (!std::in_range<size_t>(retries) || retries == std::numeric_limits<size_t>::max())
    {
        throw ArithmeticalError("HTTP_CALL expects a representable retry count, got {}", retries);
    }
    return static_cast<size_t>(retries);
}
}

void detail::checkHttpStatus(unsigned status, std::string_view endpoint, std::string_view body)
{
    if (status >= 200 && status < 300)
    {
        return;
    }
    switch (status)
    {
        case 408:
        case 429:
        case 500:
        case 502:
        case 503:
        case 504:
            throw RetryableAsyncError(
                "HTTP_CALL to {} got status {}: {}. Retrying this POST may duplicate server-side effects", endpoint, status, body);
        default:
            throw IOError("HTTP_CALL to {} got status {}: {}", endpoint, status, body);
    }
}

void detail::rethrowHttpTransportError(const boost::system::system_error& error, std::string_view url)
{
    namespace asioError = boost::asio::error;
    const auto code = error.code();
    if (code == asioError::operation_aborted)
    {
        throw;
    }
    if (code == asioError::connection_refused || code == asioError::connection_reset || code == asioError::connection_aborted
        || code == asioError::timed_out || code == asioError::network_unreachable || code == asioError::network_down
        || code == asioError::host_unreachable
#ifdef EHOSTDOWN
        || code == boost::system::error_condition{EHOSTDOWN, boost::system::generic_category()}
#endif
        || code == asioError::try_again || code == asioError::eof || code == asioError::host_not_found_try_again
        || code == boost::beast::error::timeout || code == http::error::end_of_stream)
    {
        throw RetryableAsyncError(
            "HTTP_CALL to {} failed transiently: {}. Retrying this POST may duplicate server-side effects", url, error.what());
    }
    throw IOError("HTTP_CALL to {} failed permanently: {}", url, error.what());
}

class HttpCallAsyncFunction::Prepared final : public PreparedAsyncCall
{
public:
    Prepared(
        HttpCallAsyncFunction& owner,
        std::string body,
        std::string url,
        std::string host,
        std::string port,
        std::string target,
        uint64_t millis,
        size_t retries)
        : PreparedAsyncCall(retries)
        , owner(owner)
        , body(std::move(body))
        , url(std::move(url))
        , host(std::move(host))
        , port(std::move(port))
        , target(std::move(target))
        , millis(millis)
    {
    }

    boost::asio::awaitable<SharedRuntimeValues> executeAttempt() override
    {
        co_return co_await owner.execute(body, url, host, port, target, millis);
    }

private:
    HttpCallAsyncFunction& owner;
    std::string body, url, host, port, target;
    uint64_t millis;
};

std::unique_ptr<PreparedAsyncCall> HttpCallAsyncFunction::prepare(SharedRuntimeValues arguments)
{
    PRECONDITION(arguments.size() >= 2 && arguments.size() <= 4, "HTTP_CALL takes (body, url[, timeout_ms[, retries]])");
    std::string body{arguments[0].get<std::string_view>()};
    std::string url{arguments[1].get<std::string_view>()};
    auto millis = static_cast<uint64_t>(DEFAULT_TIMEOUT.count());
    if (arguments.size() >= 3)
    {
        millis = checkedUnsignedIntegerOption(arguments[2], "HTTP_CALL timeout");
    }
    if (millis == 0)
    {
        throw ArithmeticalError("HTTP_CALL expects a positive timeout in milliseconds, got {}", millis);
    }
    static_cast<void>(checkedSteadyDeadline(millis, "HTTP_CALL"));
    const auto parsed = boost::urls::parse_uri(url);
    if (!parsed || parsed->scheme() != "http" || parsed->host().empty())
    {
        throw FormattingError("HTTP_CALL needs an http:// url with a host, got {}", url);
    }
    std::string host{parsed->host()};
    std::string port = parsed->has_port() ? std::string{parsed->port()} : "80";
    std::string target = parsed->encoded_target().empty() ? "/" : std::string{parsed->encoded_target()};
    const auto retries = parseRetries(arguments);
    return std::make_unique<Prepared>(
        *this, std::move(body), std::move(url), std::move(host), std::move(port), std::move(target), millis, retries);
}

boost::asio::awaitable<SharedRuntimeValues>
HttpCallAsyncFunction::execute(std::string body, std::string url, std::string host, std::string port, std::string target, uint64_t millis)
{
    const auto deadline = checkedSteadyDeadline(millis, "HTTP_CALL");
    try
    {
        co_return SharedRuntimeValues::make(
            co_await exchange(std::move(host), std::move(port), std::move(target), std::move(body), deadline));
    }
    catch (const boost::system::system_error& error)
    {
        stream.reset();
        detail::rethrowHttpTransportError(error, url);
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
    detail::checkHttpStatus(response.result_int(), host + ":" + port + target, response.body());
    co_return std::move(response.body());
}

/// NOLINTEND(clang-analyzer-core.CallAndMessage)

}
