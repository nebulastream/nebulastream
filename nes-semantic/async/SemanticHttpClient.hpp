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
#include <optional>
#include <string>
#include <boost/asio/awaitable.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/core/tcp_stream.hpp>

namespace NES
{

/// The answer of one POST: its status and body. Only a failed exchange throws; a non-2xx status is the caller's to judge.
struct SemanticHttpResponse
{
    unsigned status;
    std::string body;
    /// The `Retry-After` header, if the response has one.
    std::optional<std::string> retryAfter;
};

/// A minimal awaitable HTTP/1.1 client for one OpenAI-compatible endpoint, following `HttpCallAsyncFunction`: it keeps
/// its connection open between requests and reopens a dropped keep-alive connection once.
///
/// ponytail: http:// only, as HTTP_CALL; hosted models need a beast::ssl_stream.
class SemanticHttpClient
{
public:
    /// Throws `InvalidSemanticModel` for a url that is not http://host[:port][/path].
    SemanticHttpClient(const std::string& url, std::optional<std::string> bearerToken, std::chrono::milliseconds timeout);

    /// POSTs `body` as `application/json`. Throws `CannotReadFromSource` when no exchange happens.
    boost::asio::awaitable<SemanticHttpResponse> post(const std::string& body);

private:
    boost::asio::awaitable<SemanticHttpResponse> exchange(const std::string& body);

    std::string host;
    std::string port;
    std::string target;
    std::optional<std::string> bearerToken;
    std::chrono::milliseconds timeout;
    std::optional<boost::beast::tcp_stream> stream;
    boost::beast::flat_buffer buffer;
};

}
