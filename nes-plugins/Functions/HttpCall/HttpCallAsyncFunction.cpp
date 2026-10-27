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

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <Util/Strings.hpp>
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

/// Bounds the connect and, separately, the exchange, unless the call passes its own timeout.
constexpr std::chrono::milliseconds DEFAULT_TIMEOUT = std::chrono::seconds(30);

/// The server closed the kept-alive connection before it answered. A timeout or a cancellation is not such a close: the
/// server may have received the request, so sending it again could repeat it.
bool closedByPeer(const boost::system::error_code& code)
{
    return code == http::error::end_of_stream || code == boost::asio::error::eof || code == boost::asio::error::connection_reset
        || code == boost::asio::error::broken_pipe;
}
}

AsyncFunctionRegistryReturnType HttpCallAsyncFunction::create(AsyncFunctionRegistryArguments)
{
    return std::make_unique<HttpCallAsyncFunction>();
}

boost::asio::awaitable<std::string> HttpCallAsyncFunction::call(std::vector<std::string> arguments)
{
    PRECONDITION(arguments.size() == 2 || arguments.size() == 3, "HTTP_CALL takes (body, url[, timeout_ms])");
    const auto& url = arguments[1];
    auto timeout = DEFAULT_TIMEOUT;
    if (arguments.size() == 3)
    {
        const auto millis = from_chars<uint64_t>(arguments[2]);
        if (!millis || *millis == 0)
        {
            throw CannotReadFromSource("HTTP_CALL expects a positive timeout in milliseconds, got {}", arguments[2]);
        }
        timeout = std::chrono::milliseconds{*millis};
    }
    const auto parsed = boost::urls::parse_uri(url);
    if (!parsed || parsed->scheme() != "http")
    {
        throw CannotReadFromSource("HTTP_CALL needs an http:// url, got {}", url);
    }
    const std::string host = parsed->host();
    const std::string port = parsed->has_port() ? std::string{parsed->port()} : "80";
    const std::string target = parsed->encoded_target().empty() ? "/" : std::string{parsed->encoded_target()};

    /// A kept-alive connection may have been closed by the server in the meantime; that only shows on use.
    const bool reused = stream.has_value() && connectedTo == host + ":" + port;
    try
    {
        co_return co_await exchange(host, port, target, arguments[0], timeout);
    }
    catch (const boost::system::system_error& error)
    {
        stream.reset();
        if (!reused || !closedByPeer(error.code()))
        {
            throw CannotReadFromSource("HTTP_CALL to {} failed: {}", url, error.what());
        }
    }
    try
    {
        co_return co_await exchange(host, port, target, arguments[0], timeout);
    }
    catch (const boost::system::system_error& error)
    {
        stream.reset();
        throw CannotReadFromSource("HTTP_CALL to {} failed: {}", url, error.what());
    }
}

boost::asio::awaitable<std::string> HttpCallAsyncFunction::exchange(
    const std::string& host,
    const std::string& port,
    const std::string& target,
    const std::string& body,
    const std::chrono::milliseconds timeout)
{
    using boost::asio::use_awaitable;
    if (!stream || connectedTo != host + ":" + port)
    {
        const auto executor = co_await boost::asio::this_coro::executor;
        boost::asio::ip::tcp::resolver resolver{executor};
        const auto endpoints = co_await resolver.async_resolve(host, port, use_awaitable);
        stream.emplace(executor);
        buffer.clear();
        stream->expires_after(timeout);
        co_await stream->async_connect(endpoints, use_awaitable);
        connectedTo = host + ":" + port;
    }

    http::request<http::string_body> request{http::verb::post, target, 11, body};
    request.set(http::field::host, host + ":" + port);
    request.set(http::field::content_type, "text/plain");
    request.keep_alive(true);
    request.prepare_payload();
    stream->expires_after(timeout);
    co_await http::async_write(*stream, request, use_awaitable);

    http::response<http::string_body> response;
    co_await http::async_read(*stream, buffer, response, use_awaitable);
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

}
