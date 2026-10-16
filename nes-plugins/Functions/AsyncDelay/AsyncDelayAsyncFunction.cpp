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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <Util/Strings.hpp>
#include <AsyncFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

AsyncFunctionRegistryReturnType AsyncDelayAsyncFunction::create(AsyncFunctionRegistryArguments arguments)
{
    if (arguments.configuration.empty())
    {
        return std::make_unique<AsyncDelayAsyncFunction>(1);
    }
    const auto batchSize = from_chars<uint64_t>(arguments.configuration);
    if (!batchSize || *batchSize == 0)
    {
        throw CannotOpenSource("ASYNC_DELAY expects a positive batch size, got {}", arguments.configuration);
    }
    return std::make_unique<AsyncDelayAsyncFunction>(*batchSize);
}

boost::asio::awaitable<std::vector<AsyncCallResult>> AsyncDelayAsyncFunction::callBatch(std::vector<std::vector<std::string>> rows)
{
    /// The delay is a constant, so every row of the batch asks for the same wait.
    uint64_t millis = 0;
    for (const auto& arguments : rows)
    {
        PRECONDITION(arguments.size() >= 2, "ASYNC_DELAY takes (value, millis[, batch_size])");
        const auto rowMillis = from_chars<uint64_t>(arguments[1]);
        if (!rowMillis)
        {
            throw CannotReadFromSource("ASYNC_DELAY expects a non-negative delay in milliseconds, got {}", arguments[1]);
        }
        millis = std::max(millis, *rowMillis);
    }
    boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, std::chrono::milliseconds{millis}};
    co_await timer.async_wait(boost::asio::use_awaitable);

    const auto covered = std::to_string(rows.size());
    std::vector<AsyncCallResult> results;
    results.reserve(rows.size());
    for (auto& arguments : rows)
    {
        results.push_back({std::move(arguments[0]), covered});
    }
    co_return results;
}

}
