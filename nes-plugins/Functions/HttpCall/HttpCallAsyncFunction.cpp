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

std::optional<size_t> HttpCallAsyncFunction::maxRetries(const SharedRuntimeValues& arguments) const
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
        : owner(owner)
        , body(std::move(body))
        , url(std::move(url))
        , host(std::move(host))
        , port(std::move(port))
        , target(std::move(target))
        , millis(millis)
        , retries(retries)
    {
    }

    boost::asio::awaitable<SharedRuntimeValues> call() override { co_return co_await owner.execute(body, url, host, port, target, millis); }

    std::optional<size_t> maxRetries() const override { return retries; }

private:
    HttpCallAsyncFunction& owner;
    std::string body, url, host, port, target;
    uint64_t millis;
    size_t retries;
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
    const auto retries = maxRetries(arguments).value_or(0);
    return std::make_unique<Prepared>(
        *this, std::move(body), std::move(url), std::move(host), std::move(port), std::move(target), millis, retries);
}

boost::asio::awaitable<SharedRuntimeValues> HttpCallAsyncFunction::call(SharedRuntimeValues arguments)
{
    auto prepared = prepare(std::move(arguments));
    co_return co_await prepared->call();
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
        throw IOError("HTTP_CALL to {} failed: {}", url, error.what());
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
        throw IOError("HTTP_CALL to {}:{}{} got status {}: {}", host, port, target, response.result_int(), response.body());
    }
    co_return std::move(response.body());
}

/// NOLINTEND(clang-analyzer-core.CallAndMessage)

}
