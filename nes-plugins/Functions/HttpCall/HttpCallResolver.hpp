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
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <Sources/AsyncError.hpp>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>
#include <folly/ScopeGuard.h>
#include <ErrorHandling.hpp>

namespace NES::detail
{
struct StartHttpResolution
{
    template <typename Handler>
    void operator()(boost::asio::ip::tcp::resolver& resolver, const std::string& host, const std::string& port, Handler handler) const
    {
        resolver.async_resolve(host, port, std::move(handler));
    }
};

/// Must run on a strand. Late resolver callbacks retain state and are ignored after timeout/cancellation.
template <typename StartResolve>
[[nodiscard]] boost::asio::awaitable<AsyncResult<boost::asio::ip::tcp::resolver::results_type>>
resolveHttpHost(std::string host, std::string port, const std::chrono::steady_clock::time_point deadline, StartResolve startResolve)
{
    using Clock = std::chrono::steady_clock;
    using Resolver = boost::asio::ip::tcp::resolver;
    namespace asio = boost::asio;
    if (Clock::now() >= deadline)
    {
        co_return std::unexpected(AsyncError::timedOut());
    }
    const bool previousThrowPolicy = co_await asio::this_coro::throw_if_cancelled();
    co_await asio::this_coro::throw_if_cancelled(false);

    struct State
    {
        State(const asio::any_io_executor& executor, Clock::time_point deadline) : resolver(executor), completion(executor, deadline) { }

        Resolver resolver;
        asio::steady_timer completion;
        std::optional<Resolver::results_type> endpoints;
        boost::system::error_code error;
        Clock::time_point completedAt;
        bool finished = false;
    };

    const auto state = std::make_shared<State>(co_await asio::this_coro::executor, deadline);
    auto cancelPending = folly::makeGuard(
        [state]
        {
            if (!state->finished)
            {
                state->finished = true;
                state->resolver.cancel();
            }
        });
    startResolve(
        state->resolver,
        host,
        port,
        [state](const boost::system::error_code error, Resolver::results_type endpoints)
        {
            if (state->finished)
            {
                return;
            }
            state->error = error;
            state->endpoints = std::move(endpoints);
            state->completedAt = Clock::now();
            state->finished = true;
            state->completion.cancel();
        });
    if (!state->finished)
    {
        boost::system::error_code ignored;
        co_await state->completion.async_wait(asio::redirect_error(asio::use_awaitable, ignored));
    }
    const auto cancellation = co_await asio::this_coro::cancellation_state;
    if (cancellation.cancelled() != asio::cancellation_type::none)
    {
        co_await asio::this_coro::throw_if_cancelled(previousThrowPolicy);
        co_return std::unexpected(AsyncError::cancelled());
    }
    if (!state->finished || state->completedAt > deadline)
    {
        co_await asio::this_coro::throw_if_cancelled(previousThrowPolicy);
        co_return std::unexpected(AsyncError::timedOut());
    }
    if (state->error)
    {
        auto kind = AsyncError::Type::Permanent;
        if (state->error == asio::error::operation_aborted)
        {
            kind = AsyncError::Type::Cancelled;
        }
        else if (state->error == asio::error::timed_out)
        {
            kind = AsyncError::Type::TimedOut;
        }
        else if (state->error == asio::error::host_not_found_try_again || state->error == asio::error::try_again)
        {
            kind = AsyncError::Type::Retryable;
        }
        co_await asio::this_coro::throw_if_cancelled(previousThrowPolicy);
        co_return std::unexpected(AsyncError{IOError("HTTP_CALL DNS resolution failed: {}", state->error.message()), kind});
    }
    co_await asio::this_coro::throw_if_cancelled(previousThrowPolicy);
    INVARIANT(state->endpoints.has_value(), "Successful HTTP_CALL DNS resolution has no endpoints");
    co_return std::move(*state->endpoints);
}

template <typename StartResolve = StartHttpResolution>
[[nodiscard]] boost::asio::awaitable<AsyncResult<boost::asio::ip::tcp::resolver::results_type>>
resolveHttpHost(std::string host, std::string port, const std::chrono::steady_clock::time_point deadline)
{
    return resolveHttpHost<StartResolve>(std::move(host), std::move(port), deadline, StartResolve{});
}
}
