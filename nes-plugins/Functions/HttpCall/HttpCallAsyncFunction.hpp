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
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <Sources/AsyncFunction.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/core/tcp_stream.hpp>
#include <boost/system/system_error.hpp>
#include <AsyncFunctionRegistry.hpp>

namespace NES
{
namespace detail
{
/// Internal HTTP failure policy, shared by execution and deterministic classification tests.
void checkHttpStatus(unsigned status, std::string_view endpoint, std::string_view body);
/// Must be called from a system_error catch handler; cancellation is rethrown unchanged.
[[noreturn]] void rethrowHttpTransportError(const boost::system::system_error& error, std::string_view url);
}

/// Executes `HTTP_CALL(body, url[, timeout_ms[, retries]])`: POSTs `body` to `url` and returns the response body.
/// Keeps connections open between calls, but never silently resends a failed POST.
/// Retries default to zero; explicitly requested retries can duplicate server-side effects.
/// Each attempt has one deadline covering DNS, connect, request write, and response read.
///
/// http:// only; add a beast::ssl_stream (openssl is in the image) when a service needs https
class HttpCallAsyncFunction final : public AsyncFunction
{
public:
    static AsyncFunctionRegistryReturnType create(AsyncFunctionRegistryArguments);

    std::unique_ptr<PreparedAsyncCall> prepare(SharedRuntimeValues arguments) override;

private:
    class Prepared;
    boost::asio::awaitable<SharedRuntimeValues>
    execute(std::string body, std::string url, std::string host, std::string port, std::string target, uint64_t millis);
    /// Takes its arguments by value, as a coroutine has to own what it reads after its first suspension.
    boost::asio::awaitable<std::string>
    exchange(std::string host, std::string port, std::string target, std::string body, std::chrono::steady_clock::time_point deadline);

    std::optional<boost::beast::tcp_stream> stream;
    boost::beast::flat_buffer buffer;
    std::string connectedTo;
};

}
