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
#include <Sources/AsyncOperator.hpp>

#include <expected>
#include <utility>
#include <Sources/AsyncInputAccessor.hpp>
#include <Sources/AsyncError.hpp>
#include <Sources/AsyncExecutionContext.hpp>
#include <Sources/SharedRuntimeValues.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/this_coro.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> invokeOnce(AsyncOperator op, AsyncExecutionContext& context, AsyncRowHandle row)
{
    const bool throwIfCancelled = co_await boost::asio::this_coro::throw_if_cancelled();
    co_await boost::asio::this_coro::throw_if_cancelled(false);
    auto prepared = op.prepare(context, row);
    if (!prepared)
    {
        co_await boost::asio::this_coro::throw_if_cancelled(throwIfCancelled);
        co_return std::unexpected(std::move(prepared.error()));
    }
    PRECONDITION(*prepared != nullptr, "Async operator preparation must produce a prepared call");
    auto result = co_await (*prepared)->executeAttempt();
    co_await boost::asio::this_coro::throw_if_cancelled(throwIfCancelled);
    co_return result;
}

boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> invokeOnce(AsyncOperator op, AsyncExecutionContext& context)
{
    return invokeOnce(std::move(op), context, AsyncRowHandle{});
}

boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> invokeOnce(AsyncOperator op, AsyncRowHandle row)
{
    AsyncExecutionContext context;
    co_return co_await invokeOnce(std::move(op), context, std::move(row));
}

boost::asio::awaitable<AsyncResult<SharedRuntimeValues>> invokeOnce(AsyncOperator op)
{
    return invokeOnce(std::move(op), AsyncRowHandle{});
}
}
