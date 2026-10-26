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

#include <SemanticHttpClient.hpp>

#include <chrono>
#include <optional>
#include <string>
#include <utility>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/http.hpp>
#include <boost/system/system_error.hpp>
#include <boost/url/parse.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

namespace http = boost::beast::http;

SemanticHttpClient::SemanticHttpClient(
    const std::string& url, std::optional<std::string> bearerToken, const std::chrono::milliseconds timeout)
    : bearerToken(std::move(bearerToken)), timeout(timeout)
{
    const auto parsed = boost::urls::parse_uri(url);
    if (!parsed || parsed->scheme() != "http")
    {
        throw InvalidSemanticModel("SEM_CALL needs an http:// endpoint, got {}", url);
    }
    host = parsed->host();
    port = parsed->has_port() ? std::string{parsed->port()} : "80";
    target = parsed->encoded_target().empty() ? "/" : std::string{parsed->encoded_target()};
}

boost::asio::awaitable<SemanticHttpResponse> SemanticHttpClient::post(const std::string& body)
{
    /// A kept-alive connection may have been closed by the server in the meantime; that only shows on use.
    const bool reused = stream.has_value();
    try
    {
        co_return co_await exchange(body);
    }
    catch (const boost::system::system_error& error)
    {
        stream.reset();
        if (!reused)
        {
            throw CannotReadFromSource("SEM_CALL to {}:{}{} failed: {}", host, port, target, error.what());
        }
    }
    try
    {
        co_return co_await exchange(body);
    }
    catch (const boost::system::system_error& error)
    {
        stream.reset();
        throw CannotReadFromSource("SEM_CALL to {}:{}{} failed: {}", host, port, target, error.what());
    }
}

boost::asio::awaitable<SemanticHttpResponse> SemanticHttpClient::exchange(const std::string& body)
{
    using boost::asio::use_awaitable;
    if (!stream)
    {
        const auto executor = co_await boost::asio::this_coro::executor;
        boost::asio::ip::tcp::resolver resolver{executor};
        const auto endpoints = co_await resolver.async_resolve(host, port, use_awaitable);
        stream.emplace(executor);
        buffer.clear();
        stream->expires_after(timeout);
        co_await stream->async_connect(endpoints, use_awaitable);
    }

    http::request<http::string_body> request{http::verb::post, target, 11, body};
    request.set(http::field::host, host + ":" + port);
    request.set(http::field::content_type, "application/json");
    if (bearerToken)
    {
        request.set(http::field::authorization, fmt::format("Bearer {}", *bearerToken));
    }
    request.keep_alive(true);
    request.prepare_payload();
    stream->expires_after(timeout);
    co_await http::async_write(*stream, request, use_awaitable);

    http::response_parser<http::string_body> parser;
    /// Beast limits a response body to 8 MB by default; a chat completion stays far below, but the limit is explicit here.
    parser.body_limit(64U * 1024U * 1024U);
    co_await http::async_read(*stream, buffer, parser, use_awaitable);
    auto response = parser.release();
    if (!response.keep_alive())
    {
        stream.reset();
    }
    co_return SemanticHttpResponse{.status = response.result_int(), .body = std::move(response.body())};
}

}
