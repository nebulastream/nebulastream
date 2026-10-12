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

#include <Async/HttpOperatorExecutor.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncOperatorOptions.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Http/DummyHttpServer.hpp>
#include <Http/HttpBackend.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

constexpr uint32_t POOLED_BUFFER_SIZE = 4096;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 64;
constexpr BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.5;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * POOLED_BUFFER_SIZE;

std::shared_ptr<BufferManager> makeBufferManager()
{
    return BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
}

UnqualifiedUnboundField field(const std::string& name, const DataType::Type type)
{
    return UnqualifiedUnboundField{Identifier::parse(name), type};
}

Schema<UnqualifiedUnboundField, Ordered> inputSchema()
{
    return Schema<UnqualifiedUnboundField, Ordered>{field("text", DataType::Type::VARSIZED)};
}

Schema<UnqualifiedUnboundField, Ordered> outputSchema()
{
    return Schema<UnqualifiedUnboundField, Ordered>{field("text", DataType::Type::VARSIZED), field("body", DataType::Type::VARSIZED)};
}

/// The smallest operator this base supports: the input field of the first record goes into the
/// request body, and the response body into the output field of every record.
class EchoExecutor final : public HttpOperatorExecutor<>
{
public:
    explicit EchoExecutor(const AsyncOperatorContext& context, const size_t loopThreads = 1)
        : HttpOperatorExecutor("Echo", loopThreads)
        , request(detail::httpRequestFromConfig(context, "Echo"))
        , inputFieldIndex(requiredFieldIndex(*context.inputLayout, requiredOption(context, "input_field", "Echo"), "input_field", "Echo"))
        , outputFieldIndex(
              requiredFieldIndex(*context.outputLayout, requiredOption(context, "output_field", "Echo"), "output_field", "Echo"))
    {
    }

protected:
    std::pair<HttpRequest, std::monostate> buildRequest(const std::span<const AsyncRecordView> batch) override
    {
        auto copy = request;
        copy.body = batch.front().readAsText(inputFieldIndex);
        return {std::move(copy), std::monostate{}};
    }

    std::vector<AsyncRecordResult>
    decode(const HttpResponse& response, const std::monostate&, const std::span<const AsyncRecordView> batch) override
    {
        std::vector<AsyncRecordResult> results(batch.size());
        for (auto& result : results)
        {
            result.fields.push_back(AsyncFieldValue{.fieldIndex = outputFieldIndex, .value = response.body});
        }
        return results;
    }

private:
    HttpRequest request;
    size_t inputFieldIndex;
    size_t outputFieldIndex;
};

/// Collects batch outcomes, which arrive on the loop's threads.
class Outcomes
{
    mutable std::mutex mutex;
    std::vector<AsyncBatchOutcome> results;

public:
    void add(AsyncBatchOutcome outcome)
    {
        const std::lock_guard lock{mutex};
        results.push_back(std::move(outcome));
    }

    [[nodiscard]] size_t size() const
    {
        const std::lock_guard lock{mutex};
        return results.size();
    }

    [[nodiscard]] bool waitFor(const size_t expected, const std::chrono::milliseconds deadline)
    {
        const auto until = std::chrono::steady_clock::now() + deadline;
        while (size() < expected && std::chrono::steady_clock::now() < until)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        return size() >= expected;
    }

    [[nodiscard]] std::vector<AsyncBatchOutcome> take()
    {
        const std::lock_guard lock{mutex};
        return results;
    }
};

}

class HttpOperatorExecutorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("HttpOperatorExecutorTest.log", LogLevel::LOG_DEBUG); }

    AsyncRecordLayout input{inputSchema()};
    AsyncRecordLayout output{outputSchema()};
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

    [[nodiscard]] AsyncOperatorContext contextFor(const std::string& path, std::unordered_map<std::string, std::string> extra = {}) const
    {
        std::unordered_map<std::string, std::string> config{{"url", server.url(path)}, {"input_field", "text"}, {"output_field", "body"}};
        config.insert(extra.begin(), extra.end());
        return AsyncOperatorContext{.inputLayout = &input, .outputLayout = &output, .config = std::move(config), .batchSize = 4};
    }

    static std::vector<AsyncRecordView>
    makeRecords(const AsyncRecordLayout& layout, TupleBuffer& buffer, BufferManager& bufferManager, const std::vector<std::string>& texts)
    {
        std::vector<AsyncRecordView> views;
        for (size_t index = 0; index < texts.size(); ++index)
        {
            AsyncRecordWriter writer{layout, buffer, bufferManager, index};
            writer.writeText(0, texts[index]);
            views.emplace_back(layout, buffer, index);
        }
        return views;
    }
};

TEST_F(HttpOperatorExecutorTest, SubmitsOneRequestPerBatchAndDecodesTheResponse)
{
    auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto records = makeRecords(input, buffer, *bufferManager, {"hello"});

    EchoExecutor executor{contextFor("/upper")};
    Outcomes outcomes;
    executor.submit(records, [&outcomes](auto outcome) { outcomes.add(std::move(outcome)); });

    ASSERT_TRUE(outcomes.waitFor(1, std::chrono::seconds(5)));
    const auto results = outcomes.take();
    ASSERT_TRUE(results.front().has_value());
    ASSERT_EQ(results.front()->size(), 1);
    ASSERT_EQ(results.front()->front().fields.size(), 1);
    EXPECT_EQ(results.front()->front().fields.front().value, "HELLO");
    EXPECT_EQ(results.front()->front().fields.front().fieldIndex, 1);
    EXPECT_EQ(server.requestCount(), 1);
}

TEST_F(HttpOperatorExecutorTest, AnEmptyBatchMakesNoRequest)
{
    EchoExecutor executor{contextFor("/echo")};
    Outcomes outcomes;
    executor.submit({}, [&outcomes](auto outcome) { outcomes.add(std::move(outcome)); });

    ASSERT_EQ(outcomes.size(), 1);
    ASSERT_TRUE(outcomes.take().front().has_value());
    EXPECT_EQ(server.requestCount(), 0);
}

/// The property the whole interface exists for: thirty-two calls outstanding at once against a
/// route that waits 200 ms each, and not one thread per call. The blocking interface would need
/// thirty-two threads for this.
TEST_F(HttpOperatorExecutorTest, KeepsManyCallsInFlightWithoutAThreadEach)
{
    constexpr size_t Batches = 32;
    constexpr auto PerCall = std::chrono::milliseconds{200};

    auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto records = makeRecords(input, buffer, *bufferManager, {"payload"});

    EchoExecutor executor{contextFor(fmt::format("/slow?ms={}", PerCall.count()))};
    Outcomes outcomes;

    const auto start = std::chrono::steady_clock::now();
    for (size_t batch = 0; batch < Batches; ++batch)
    {
        executor.submit(records, [&outcomes](auto outcome) { outcomes.add(std::move(outcome)); });
    }
    ASSERT_TRUE(outcomes.waitFor(Batches, std::chrono::seconds(30))) << "only " << outcomes.size() << " of " << Batches << " finished";
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_LT(elapsed, PerCall * 10) << "took " << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() << " ms";
    for (const auto& outcome : outcomes.take())
    {
        ASSERT_TRUE(outcome.has_value());
        ASSERT_EQ(outcome->size(), 1);
        EXPECT_EQ(outcome->front().fields.front().value, "payload");
    }
    EXPECT_EQ(server.requestCount(), Batches);
}

TEST_F(HttpOperatorExecutorTest, RetriesAServerErrorAccordingToTheConfiguration)
{
    auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto records = makeRecords(input, buffer, *bufferManager, {"payload"});

    /// Two refusals, then the echo: it takes the two configured retries to get there.
    EchoExecutor executor{contextFor("/flaky?n=2", {{"max_retries", "2"}})};
    Outcomes outcomes;
    executor.submit(records, [&outcomes](auto outcome) { outcomes.add(std::move(outcome)); });

    ASSERT_TRUE(outcomes.waitFor(1, std::chrono::seconds(10)));
    const auto results = outcomes.take();
    ASSERT_TRUE(results.front().has_value());
    EXPECT_EQ(results.front()->front().fields.front().value, "payload");
    EXPECT_EQ(server.requestCount(), 3);
}

/// A transport that gave up fails the query rather than filling in a default. The failure travels
/// in the outcome, because a completion cannot throw at the framework.
TEST_F(HttpOperatorExecutorTest, AnUnreachableEndpointYieldsAFailureOutcome)
{
    auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto records = makeRecords(input, buffer, *bufferManager, {"a"});

    auto context = contextFor("/echo");
    /// Nothing listens on port 1 of the loopback interface.
    context.config["url"] = "http://127.0.0.1:1/echo";
    EchoExecutor executor{context};
    Outcomes outcomes;
    executor.submit(records, [&outcomes](auto outcome) { outcomes.add(std::move(outcome)); });

    ASSERT_TRUE(outcomes.waitFor(1, std::chrono::seconds(10)));
    auto results = outcomes.take();
    ASSERT_FALSE(results.front().has_value());
    ASSERT_EXCEPTION_ERRORCODE(std::rethrow_exception(results.front().error()), ErrorCode::HttpRequestFailed);
}

TEST_F(HttpOperatorExecutorTest, AConfigurationWithoutAUrlIsRejected)
{
    auto context = contextFor("/echo");
    context.config.erase("url");
    ASSERT_EXCEPTION_ERRORCODE((EchoExecutor{context}), ErrorCode::InvalidConfigParameter);
}

TEST_F(HttpOperatorExecutorTest, ANonNumericTimeoutIsRejected)
{
    const auto context = contextFor("/echo", {{"timeout_seconds", "soon"}});
    ASSERT_EXCEPTION_ERRORCODE((EchoExecutor{context}), ErrorCode::InvalidConfigParameter);
}

TEST_F(HttpOperatorExecutorTest, AnApiKeyIsReadFromTheEnvironmentAndNotFromTheConfiguration)
{
    ::setenv("HTTP_OPERATOR_EXECUTOR_TEST_KEY", "secret-value", 1);
    const auto request = detail::httpRequestFromConfig(contextFor("/echo", {{"api_key_env", "HTTP_OPERATOR_EXECUTOR_TEST_KEY"}}), "Echo");

    ASSERT_EQ(request.headers.size(), 1);
    EXPECT_EQ(request.headers.front().first, "Authorization");
    EXPECT_EQ(request.headers.front().second, "Bearer secret-value");
    ::unsetenv("HTTP_OPERATOR_EXECUTOR_TEST_KEY");
}

TEST_F(HttpOperatorExecutorTest, AMissingApiKeyVariableIsReported)
{
    ::unsetenv("HTTP_OPERATOR_EXECUTOR_TEST_ABSENT");
    ASSERT_EXCEPTION_ERRORCODE(
        std::ignore = detail::httpRequestFromConfig(contextFor("/echo", {{"api_key_env", "HTTP_OPERATOR_EXECUTOR_TEST_ABSENT"}}), "Echo"),
        ErrorCode::InvalidConfigParameter);
}

}
