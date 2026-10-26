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
#include <vector>
#include <Sources/AsyncFunction.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <AsyncFunctionRegistry.hpp>

namespace NES
{

/// Executes `HTTP_CALL(body, url[, timeout_ms])`: POSTs `body` to `url` and returns the response body.
/// Keeps its connection open between calls; a dropped keep-alive connection is reopened once.
///
/// ponytail: http:// only; add a beast::ssl_stream (openssl is in the image) when a service needs https
class HttpCallAsyncFunction final : public AsyncFunction
{
public:
    static AsyncFunctionRegistryReturnType create(AsyncFunctionRegistryArguments);

    boost::asio::awaitable<std::string> call(std::vector<std::string> arguments) override;

private:
    /// Takes its arguments by value, as a coroutine has to own what it reads after its first suspension.
    boost::asio::awaitable<std::string>
    exchange(std::string host, std::string port, std::string target, std::string body, std::chrono::milliseconds timeout);

    std::optional<boost::beast::tcp_stream> stream;
    boost::beast::flat_buffer buffer;
    std::string connectedTo;
};

}
