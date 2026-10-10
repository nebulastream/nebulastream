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

#include <Http/HttpEventLoop.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <expected>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Http/DummyHttpServer.hpp>
#include <Http/HttpBackend.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{

namespace
{

HttpRequest post(std::string url, std::string body, const size_t maxRetries = 0)
{
    return HttpRequest{
        .url = std::move(url),
        .method = HttpMethod::POST,
        .headers = {},
        .body = std::move(body),
        .timeout = std::chrono::seconds(10),
        .connectTimeout = std::chrono::milliseconds(2000),
        .maxRetries = maxRetries};
}

/// Collects outcomes from the loop's threads.
class Outcomes
{
    mutable std::mutex mutex;
    std::vector<std::expected<HttpResponse, HttpError>> results;

public:
    void add(std::expected<HttpResponse, HttpError> outcome)
    {
        const std::lock_guard lock{mutex};
        results.push_back(std::move(outcome));
    }

    [[nodiscard]] size_t size() const
    {
        const std::lock_guard lock{mutex};
        return results.size();
    }

    /// Waits for `expected` outcomes and returns whether they arrived in time.
    [[nodiscard]] bool waitFor(const size_t expected, const std::chrono::milliseconds deadline)
    {
        const auto until = std::chrono::steady_clock::now() + deadline;
        while (size() < expected && std::chrono::steady_clock::now() < until)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        return size() >= expected;
    }

    [[nodiscard]] std::vector<std::expected<HttpResponse, HttpError>> take()
    {
        const std::lock_guard lock{mutex};
        return results;
    }
};

}

class HttpEventLoopTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("HttpEventLoopTest.log", LogLevel::LOG_DEBUG); }

    DummyHttpServer server;

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        server.start();
    }

    void TearDown() override
    {
        server.stop();
        BaseUnitTest::TearDown();
    }
};

TEST_F(HttpEventLoopTest, SendsARequestAndReturnsTheResponse)
{
    HttpEventLoop loop{1};
    Outcomes outcomes;

    loop.send(post(server.url("/upper"), "hello"), [&outcomes](auto outcome) { outcomes.add(std::move(outcome)); });

    ASSERT_TRUE(outcomes.waitFor(1, std::chrono::seconds(5)));
    const auto results = outcomes.take();
    ASSERT_TRUE(results.front().has_value()) << results.front().error().message;
    EXPECT_EQ(results.front()->status, 200);
    EXPECT_EQ(results.front()->body, "HELLO");
    EXPECT_EQ(loop.inFlight(), 0);
}

/// The whole point of the loop. Sixty-four requests against a route that waits 200 ms each, on a
/// single thread: they overlap, so the batch takes about one delay. The blocking client would need
/// sixty-four threads to do this, and with one thread it would take sixty-four delays.
TEST_F(HttpEventLoopTest, KeepsManyRequestsInFlightOnOneThread)
{
    constexpr size_t Requests = 64;
    constexpr auto PerRequest = std::chrono::milliseconds{200};

    HttpEventLoop loop{1};
    Outcomes outcomes;

    const auto start = std::chrono::steady_clock::now();
    for (size_t request = 0; request < Requests; ++request)
    {
        loop.send(
            post(server.url(fmt::format("/slow?ms={}", PerRequest.count())), fmt::format("body-{}", request)),
            [&outcomes](auto outcome) { outcomes.add(std::move(outcome)); });
    }
    ASSERT_TRUE(outcomes.waitFor(Requests, std::chrono::seconds(30))) << "only " << outcomes.size() << " of " << Requests << " finished";
    const auto elapsed = std::chrono::steady_clock::now() - start;

    /// Serially this would be 12.8 seconds. The bound is generous because the dummy server spawns a
    /// thread per connection, but it is far below anything serial.
    EXPECT_LT(elapsed, PerRequest * 10) << "took " << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() << " ms";

    /// Every answer is its own request's body: the transfers do not get mixed up.
    std::unordered_map<std::string, size_t> bodies;
    for (const auto& outcome : outcomes.take())
    {
        ASSERT_TRUE(outcome.has_value()) << outcome.error().message;
        bodies[outcome->body] += 1;
    }
    EXPECT_EQ(bodies.size(), Requests);
    for (size_t request = 0; request < Requests; ++request)
    {
        EXPECT_EQ(bodies[fmt::format("body-{}", request)], 1) << "missing or duplicated answer for request " << request;
    }
    EXPECT_EQ(loop.inFlight(), 0);
}

TEST_F(HttpEventLoopTest, RetriesServerErrorsOnATimerAndThenSucceeds)
{
    HttpEventLoop loop{1};
    Outcomes outcomes;

    loop.send(post(server.url("/flaky?n=2"), "payload", 2), [&outcomes](auto outcome) { outcomes.add(std::move(outcome)); });

    ASSERT_TRUE(outcomes.waitFor(1, std::chrono::seconds(10)));
    const auto results = outcomes.take();
    ASSERT_TRUE(results.front().has_value()) << results.front().error().message;
    EXPECT_EQ(results.front()->status, 200);
    EXPECT_EQ(results.front()->body, "payload");
    /// Two refusals and the answer, so the backoff really ran rather than the attempts being lost.
    EXPECT_EQ(server.requestCount(), 3);
}

TEST_F(HttpEventLoopTest, ReportsANonSuccessStatusAsAResponse)
{
    HttpEventLoop loop{1};
    Outcomes outcomes;

    loop.send(post(server.url("/status?code=404"), "x"), [&outcomes](auto outcome) { outcomes.add(std::move(outcome)); });

    ASSERT_TRUE(outcomes.waitFor(1, std::chrono::seconds(5)));
    const auto results = outcomes.take();
    ASSERT_TRUE(results.front().has_value()) << results.front().error().message;
    EXPECT_EQ(results.front()->status, 404);
}

TEST_F(HttpEventLoopTest, ReportsAnUnreachableEndpoint)
{
    HttpEventLoop loop{1};
    Outcomes outcomes;

    loop.send(post("http://127.0.0.1:1/x", "x"), [&outcomes](auto outcome) { outcomes.add(std::move(outcome)); });

    ASSERT_TRUE(outcomes.waitFor(1, std::chrono::seconds(5)));
    const auto results = outcomes.take();
    ASSERT_FALSE(results.front().has_value());
    EXPECT_NE(results.front().error().message.find("127.0.0.1:1"), std::string::npos) << results.front().error().message;
}

TEST_F(HttpEventLoopTest, HonoursTheRequestTimeout)
{
    HttpEventLoop loop{1};
    Outcomes outcomes;

    auto request = post(server.url("/stall?ms=1500"), "x");
    request.timeout = std::chrono::seconds(1);
    loop.send(std::move(request), [&outcomes](auto outcome) { outcomes.add(std::move(outcome)); });

    const auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(outcomes.waitFor(1, std::chrono::seconds(5)));
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_FALSE(outcomes.take().front().has_value());
    EXPECT_LT(elapsed, std::chrono::milliseconds(2500))
        << "took " << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() << " ms";
}

/// A caller whose completion never runs waits forever, so stopping has to answer everything it
/// abandons. This is what makes it safe for an operator to be torn down with calls outstanding.
TEST_F(HttpEventLoopTest, StoppingAnswersEverythingInFlight)
{
    constexpr size_t Requests = 8;
    Outcomes outcomes;
    {
        HttpEventLoop loop{1};
        for (size_t request = 0; request < Requests; ++request)
        {
            loop.send(post(server.url("/slow?ms=1500"), "x"), [&outcomes](auto outcome) { outcomes.add(std::move(outcome)); });
        }
        /// Let them reach the wire before pulling the rug out.
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        loop.stop();
        EXPECT_EQ(loop.inFlight(), 0);
    }

    ASSERT_EQ(outcomes.size(), Requests) << "a completion was dropped";
    for (const auto& outcome : outcomes.take())
    {
        EXPECT_FALSE(outcome.has_value());
    }
}

TEST_F(HttpEventLoopTest, RunsWorkOnATimerWithoutOccupyingAThread)
{
    HttpEventLoop loop{1};
    std::atomic<bool> ran{false};

    const auto start = std::chrono::steady_clock::now();
    loop.after(std::chrono::milliseconds(150), [&ran] { ran.store(true); });

    while (!ran.load() && std::chrono::steady_clock::now() - start < std::chrono::seconds(5))
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_TRUE(ran.load());
    EXPECT_GE(elapsed, std::chrono::milliseconds(140));
}

TEST_F(HttpEventLoopTest, SendingAfterStopAnswersImmediately)
{
    HttpEventLoop loop{1};
    loop.stop();

    Outcomes outcomes;
    loop.send(post(server.url("/echo"), "x"), [&outcomes](auto outcome) { outcomes.add(std::move(outcome)); });

    ASSERT_EQ(outcomes.size(), 1);
    EXPECT_FALSE(outcomes.take().front().has_value());
    EXPECT_EQ(server.requestCount(), 0);
}

}
