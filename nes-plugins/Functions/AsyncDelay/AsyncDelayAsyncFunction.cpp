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

#include <AsyncDelayAsyncFunction.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <Util/Strings.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <AsyncFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

AsyncFunctionRegistryReturnType AsyncDelayAsyncFunction::create(AsyncFunctionRegistryArguments arguments)
{
    if (arguments.configuration.empty())
    {
        return std::make_unique<AsyncDelayAsyncFunction>(std::nullopt);
    }
    const auto limit = from_chars<uint64_t>(arguments.configuration);
    if (!limit || *limit == 0)
    {
        throw CannotOpenSource("ASYNC_DELAY expects a positive concurrency limit, got {}", arguments.configuration);
    }
    return std::make_unique<AsyncDelayAsyncFunction>(*limit);
}

boost::asio::awaitable<AsyncCallResult> AsyncDelayAsyncFunction::callValues(std::vector<std::string> arguments)
{
    PRECONDITION(arguments.size() >= 2, "ASYNC_DELAY takes (value, millis[, max_concurrency])");
    const auto millis = from_chars<uint64_t>(arguments[1]);
    if (!millis)
    {
        throw CannotReadFromSource("ASYNC_DELAY expects a non-negative delay in milliseconds, got {}", arguments[1]);
    }
    boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, std::chrono::milliseconds{*millis}};
    co_await timer.async_wait(boost::asio::use_awaitable);
    co_return AsyncCallResult{std::move(arguments[0]), std::to_string(*millis)};
}

}
