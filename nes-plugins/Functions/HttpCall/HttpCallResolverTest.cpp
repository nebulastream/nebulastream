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
/// NOLINTBEGIN(readability-magic-numbers) Numeric values are fixture data in these tests.
#include <chrono>
#include <future>
#include <memory>
#include <utility>
#include <Sources/AsyncError.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_future.hpp>
#include <gtest/gtest.h>
#include <HttpCallResolver.hpp>

namespace NES
{
namespace
{
namespace asio = boost::asio;
using Clock = std::chrono::steady_clock;
using Resolver = asio::ip::tcp::resolver;
using namespace std::chrono_literals;

Resolver::results_type endpoints()
{
    return Resolver::results_type::create(asio::ip::tcp::endpoint{asio::ip::address_v4::loopback(), 80}, "localhost", "80");
}

template <typename Body>
void run(Body body)
{
    asio::io_context context;
    auto executor = asio::make_strand(context);
    auto future = asio::co_spawn(executor, body(executor), asio::use_future);
    context.run();
    future.get();
}
}

/// NOLINTBEGIN(misc-include-cleaner) Verified public providers; cleaner attribution false positive.
/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(HttpCallResolverTest, DefaultResolverResolvesLoopbackWithinTheDeadline)
{
    run(
        [](auto) -> asio::awaitable<void>
        {
            const auto result = co_await detail::resolveHttpHost("127.0.0.1", "80", Clock::now() + 1s);
            EXPECT_TRUE(result);
            if (!result)
            {
                co_return;
            }
            EXPECT_FALSE(result->empty());
            EXPECT_EQ(result->begin()->endpoint().address(), asio::ip::address_v4::loopback());
        });
}
/// NOLINTEND(misc-include-cleaner)
/// NOLINTEND(misc-include-cleaner)

/// NOLINTBEGIN(misc-include-cleaner) Verified public providers; cleaner attribution false positive.
/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(HttpCallResolverTest, DeadlineReturnsBeforeAnUninterruptibleDnsCompletionAndLateCompletionIsSafe)
{
    bool completed = false;
    run(
        [&](auto executor) -> asio::awaitable<void>
        {
            const auto started = Clock::now();
            const auto slowDns = [&](auto&, const auto&, const auto&, auto handler)
            {
                auto timer = std::make_shared<asio::steady_timer>(executor, 200ms);
                timer->async_wait(
                    [timer, handler = std::move(handler), &completed](auto error) mutable
                    {
                        completed = true;
                        handler(error, endpoints());
                    });
            };
            const auto result = co_await detail::resolveHttpHost("slow.test", "80", started + 10ms, slowDns);
            EXPECT_FALSE(result);
            if (result)
            {
                co_return;
            }
            EXPECT_EQ(result.error().kind, AsyncError::Type::TimedOut);
            EXPECT_FALSE(completed);
            EXPECT_LT(Clock::now() - started, 150ms);
        });
    EXPECT_TRUE(completed);
}
/// NOLINTEND(misc-include-cleaner)
/// NOLINTEND(misc-include-cleaner)

/// NOLINTBEGIN(misc-include-cleaner) Verified public providers; cleaner attribution false positive.
/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(HttpCallResolverTest, InlineSuccessAndDnsFailureDoNotMissCompletion)
{
    run(
        [](auto) -> asio::awaitable<void>
        {
            const auto success = [](auto&, const auto&, const auto&, auto handler) { handler({}, endpoints()); };
            const auto result = co_await detail::resolveHttpHost("localhost", "80", Clock::now() + 1s, success);
            EXPECT_TRUE(result);
            if (!result)
            {
                co_return;
            }
            EXPECT_EQ(result->size(), 1);
            const auto failure = [](auto&, const auto&, const auto&, auto handler) { handler(asio::error::host_not_found, {}); };
            const auto failed = co_await detail::resolveHttpHost("missing.test", "80", Clock::now() + 1s, failure);
            EXPECT_FALSE(failed);
            if (failed)
            {
                co_return;
            }
            EXPECT_EQ(failed.error().kind, AsyncError::Type::Permanent);
            const auto temporaryFailure
                = [](auto&, const auto&, const auto&, auto handler) { handler(asio::error::host_not_found_try_again, {}); };
            const auto temporary = co_await detail::resolveHttpHost("temporary.test", "80", Clock::now() + 1s, temporaryFailure);
            EXPECT_FALSE(temporary);
            if (!temporary)
            {
                EXPECT_EQ(temporary.error().kind, AsyncError::Type::Retryable);
            }
        });
}
/// NOLINTEND(misc-include-cleaner)
/// NOLINTEND(misc-include-cleaner)

/// NOLINTBEGIN(misc-include-cleaner) Verified public providers; cleaner attribution false positive.
/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(HttpCallResolverTest, ExpiredDeadlineDoesNotStartDns)
{
    run(
        [](auto) -> asio::awaitable<void>
        {
            bool started = false;
            const auto dns = [&](auto&, const auto&, const auto&, auto) { started = true; };
            const auto result = co_await detail::resolveHttpHost("localhost", "80", Clock::now() - 1ms, dns);
            EXPECT_FALSE(result);
            if (result)
            {
                co_return;
            }
            EXPECT_EQ(result.error().kind, AsyncError::Type::TimedOut);
            EXPECT_FALSE(started);
        });
}
/// NOLINTEND(misc-include-cleaner)
/// NOLINTEND(misc-include-cleaner)

/// NOLINTBEGIN(misc-include-cleaner) Verified public providers; cleaner attribution false positive.
/// NOLINTBEGIN(misc-include-cleaner) Verified public provider; false-positive attribution for this test callsite.
TEST(HttpCallResolverTest, CallerCancellationIsNotReportedAsATimeout)
{
    asio::io_context context;
    const auto executor = asio::make_strand(context);
    asio::cancellation_signal stop;
    asio::steady_timer stopTimer{executor, 5ms};
    stopTimer.async_wait([&](auto) { stop.emit(asio::cancellation_type::terminal); });
    const auto body = [&]() -> asio::awaitable<void>
    {
        const auto slowDns = [&](auto&, const auto&, const auto&, auto handler)
        {
            auto timer = std::make_shared<asio::steady_timer>(executor, 100ms);
            timer->async_wait([timer, handler = std::move(handler)](auto error) mutable { handler(error, endpoints()); });
        };
        const auto result = co_await detail::resolveHttpHost("slow.test", "80", Clock::now() + 1s, slowDns);
        EXPECT_FALSE(result);
        EXPECT_EQ(result.error().kind, AsyncError::Type::Cancelled);
    };
    auto future = asio::co_spawn(executor, body(), asio::bind_cancellation_slot(stop.slot(), asio::use_future));
    context.run();
    future.get();
}
/// NOLINTEND(misc-include-cleaner)
/// NOLINTEND(misc-include-cleaner)

}
/// NOLINTEND(readability-magic-numbers)
