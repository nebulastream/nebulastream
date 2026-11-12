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
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>
#include <folly/ScopeGuard.h>

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

/// Runs on one strand. A resolver cancellation need not interrupt an OS getaddrinfo already in progress.
/// Keep its state alive in the completion handler, but do not hold the row's coroutine past the deadline.
/// The initiation function is injectable so tests can exercise stalled and late DNS completions deterministically.
template <typename StartResolve = StartHttpResolution>
boost::asio::awaitable<boost::asio::ip::tcp::resolver::results_type>
resolveHttpHost(std::string host, std::string port, const std::chrono::steady_clock::time_point deadline, StartResolve startResolve = {})
{
    using Clock = std::chrono::steady_clock;
    using Resolver = boost::asio::ip::tcp::resolver;
    if (Clock::now() >= deadline)
    {
        throw boost::system::system_error{boost::asio::error::timed_out};
    }

    struct State
    {
        State(const boost::asio::any_io_executor& executor, const Clock::time_point deadline)
            : resolver(executor), completion(executor, deadline)
        {
        }

        Resolver resolver;
        boost::asio::steady_timer completion;
        std::optional<Resolver::results_type> endpoints;
        boost::system::error_code error;
        Clock::time_point completedAt{};
        bool finished = false;
    };

    const auto state = std::make_shared<State>(co_await boost::asio::this_coro::executor, deadline);
    const auto cancelPending = folly::makeGuard(
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
        co_await state->completion.async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, ignored));
    }
    if ((co_await boost::asio::this_coro::cancellation_state).cancelled() != boost::asio::cancellation_type::none)
    {
        throw boost::system::system_error{boost::asio::error::operation_aborted};
    }
    if (!state->finished || state->completedAt > deadline)
    {
        throw boost::system::system_error{boost::asio::error::timed_out};
    }
    if (state->error)
    {
        throw boost::system::system_error{state->error};
    }
    co_return std::move(*state->endpoints);
}

}
