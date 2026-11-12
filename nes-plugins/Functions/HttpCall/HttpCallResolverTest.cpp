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
#include <chrono>
#include <future>
#include <memory>
#include <utility>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/system/system_error.hpp>
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

TEST(HttpCallResolverTest, DefaultResolverResolvesLoopbackWithinTheDeadline)
{
    run(
        [](auto) -> asio::awaitable<void>
        {
            const auto result = co_await detail::resolveHttpHost("127.0.0.1", "80", Clock::now() + 1s);
            EXPECT_FALSE(result.empty());
            EXPECT_EQ(result.begin()->endpoint().address(), asio::ip::address_v4::loopback());
        });
}

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
            try
            {
                static_cast<void>(co_await detail::resolveHttpHost("slow.test", "80", started + 10ms, slowDns));
                ADD_FAILURE() << "DNS must time out";
            }
            catch (const boost::system::system_error& error)
            {
                EXPECT_EQ(error.code(), asio::error::timed_out);
                EXPECT_FALSE(completed);
                EXPECT_LT(Clock::now() - started, 150ms);
            }
        });
    EXPECT_TRUE(completed);
}

TEST(HttpCallResolverTest, InlineSuccessAndDnsFailureDoNotMissCompletion)
{
    run(
        [](auto) -> asio::awaitable<void>
        {
            const auto success = [](auto&, const auto&, const auto&, auto handler) { handler({}, endpoints()); };
            const auto result = co_await detail::resolveHttpHost("localhost", "80", Clock::now() + 1s, success);
            EXPECT_EQ(result.size(), 1);
            const auto failure = [](auto&, const auto&, const auto&, auto handler) { handler(asio::error::host_not_found, {}); };
            try
            {
                static_cast<void>(co_await detail::resolveHttpHost("missing.test", "80", Clock::now() + 1s, failure));
                ADD_FAILURE() << "DNS failure must propagate";
            }
            catch (const boost::system::system_error& error)
            {
                EXPECT_EQ(error.code(), asio::error::host_not_found);
            }
        });
}

TEST(HttpCallResolverTest, ExpiredDeadlineDoesNotStartDns)
{
    run(
        [](auto) -> asio::awaitable<void>
        {
            bool started = false;
            const auto dns = [&](auto&, const auto&, const auto&, auto) { started = true; };
            try
            {
                static_cast<void>(co_await detail::resolveHttpHost("localhost", "80", Clock::now() - 1ms, dns));
                ADD_FAILURE() << "An expired deadline must fail";
            }
            catch (const boost::system::system_error& error)
            {
                EXPECT_EQ(error.code(), asio::error::timed_out);
                EXPECT_FALSE(started);
            }
        });
}

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
        try
        {
            static_cast<void>(co_await detail::resolveHttpHost("slow.test", "80", Clock::now() + 1s, slowDns));
            ADD_FAILURE() << "DNS wait must be cancelled";
        }
        catch (const boost::system::system_error& error)
        {
            EXPECT_EQ(error.code(), asio::error::operation_aborted);
        }
    };
    auto future = asio::co_spawn(executor, body(), asio::bind_cancellation_slot(stop.slot(), asio::use_future));
    context.run();
    future.get();
}

}
