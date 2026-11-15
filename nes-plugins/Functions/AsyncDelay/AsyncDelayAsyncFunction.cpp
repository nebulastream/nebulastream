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
#include <AsyncDelayAsyncFunction.hpp>

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <Sources/AsyncFunction.hpp>
#include <Util/CheckedDeadline.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <AsyncFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

AsyncFunctionRegistryReturnType AsyncDelayAsyncFunction::create(AsyncFunctionRegistryArguments arguments)
{
    const auto& binding = arguments.binding;
    std::optional<size_t> bound;
    if (binding.constantArguments.size() > 2 && binding.constantArguments[2])
    {
        const auto limit = binding.constantArguments[2]->view().getUnsignedInteger();
        PRECONDITION(limit > 0 && std::in_range<size_t>(limit), "ASYNC_DELAY binding concurrency must be logically validated");
        bound = static_cast<size_t>(limit);
    }
    return std::make_unique<AsyncDelayAsyncFunction>(bound);
}

namespace
{
class PreparedDelayCall final : public PreparedAsyncCall
{
public:
    PreparedDelayCall(RuntimeValue value, uint64_t millis) : value(std::move(value)), millis(millis) { }

    boost::asio::awaitable<SharedRuntimeValues> executeAttempt() override
    {
        const auto deadline = checkedSteadyDeadline(millis, "ASYNC_DELAY");
        boost::asio::steady_timer timer{co_await boost::asio::this_coro::executor, deadline};
        co_await timer.async_wait(boost::asio::use_awaitable);
        co_return SharedRuntimeValues{value, RuntimeValue{millis}};
    }

private:
    RuntimeValue value;
    uint64_t millis;
};
}

std::unique_ptr<PreparedAsyncCall> AsyncDelayAsyncFunction::prepare(SharedRuntimeValues arguments)
{
    PRECONDITION(arguments.size() == 2 || arguments.size() == 3, "ASYNC_DELAY takes (value, millis[, max_concurrency])");
    const auto millis = checkedUnsignedIntegerOption(arguments[1], "ASYNC_DELAY millis");
    static_cast<void>(checkedSteadyDeadline(millis, "ASYNC_DELAY"));
    return std::make_unique<PreparedDelayCall>(arguments[0].toOwned(), millis);
}

}
