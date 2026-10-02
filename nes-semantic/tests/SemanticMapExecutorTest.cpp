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
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <AsyncExecutorRegistry.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SemanticAsyncWiring.hpp>
#include <SemanticModelCatalog.hpp>

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
        TOTAL_MEMORY_IN_BYTES, UNPOOLED_MEMORY_FRACTION, BUFFER_ALIGNMENT, POOLED_BUFFER_SIZE, std::make_shared<NesDefaultMemoryAllocator>());
}

UnqualifiedUnboundField field(const std::string& name, const DataType::Type type)
{
    return UnqualifiedUnboundField{Identifier::parse(name), type};
}

/// The mock backend reads the endpoint as a behaviour: 'echo' answers with the row's text
/// upper-cased, 'unparseable' with prose, 'fail' not at all.
SemanticModelConfig configWith(const std::string& behaviour, std::vector<std::string> outputValues = {}, std::string defaultValue = {})
{
    return SemanticModelConfig{
        .endpoint = behaviour,
        .modelName = "mock-model",
        .datasetPrompt = {},
        .steps = {SemanticStep{
            .kind = SemanticStep::Kind::MAP,
            .prompt = "Classify the sentiment",
            .outputColumn = "SENTIMENT",
            .outputValues = std::move(outputValues),
            .defaultValue = std::move(defaultValue)}},
        .payloadFormat = PayloadFormat::SPACE_JOINED,
        .batchSize = 4,
        .maxConcurrency = 4,
        .maxRetries = 0,
        .maxWaitTime = std::chrono::milliseconds{1000},
        .requestTimeout = std::chrono::seconds{600},
        .apiKeyEnvVar = std::nullopt,
        .backend = "mock",
        .execution = SemanticExecution::ASYNCHRONOUS};
}
}

class SemanticMapExecutorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SemanticMapExecutorTest.log", LogLevel::LOG_DEBUG); }

    AsyncRecordLayout input{Schema<UnqualifiedUnboundField, Ordered>{field("description", DataType::Type::VARSIZED)}};
    AsyncRecordLayout output{Schema<UnqualifiedUnboundField, Ordered>{
        field("description", DataType::Type::VARSIZED), field("sentiment", DataType::Type::VARSIZED)}};

    static std::unordered_map<std::string, std::string> encoded(const SemanticModelConfig& config)
    {
        return {
            {std::string{SemanticMapConfigKey},
             encodeSemanticMapPayload(
                 SemanticMapAsyncPayload{.config = config, .inputFields = {"DESCRIPTION"}, .outputFields = {"SENTIMENT"}})}};
    }

    std::unique_ptr<AsyncOperatorExecutor> makeExecutor(const SemanticModelConfig& config) const
    {
        const auto factory = AsyncExecutorRegistry::instance().find("SemanticMap");
        INVARIANT(factory.has_value(), "SemanticMap executor is not registered");
        return (*factory)(
            AsyncExecutorRegistryArguments{
                AsyncOperatorContext{
                    .inputLayout = &input, .outputLayout = &output, .config = encoded(config), .batchSize = config.batchSize}});
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

TEST_F(SemanticMapExecutorTest, PayloadSurvivesEncoding)
{
    auto config = configWith("http://localhost:8000/v1", {"POSITIVE", "NEGATIVE"}, "NEUTRAL");
    config.datasetPrompt = "Movie reviews";
    config.payloadFormat = PayloadFormat::JSON_OBJECT;
    config.apiKeyEnvVar = "SOME_API_KEY";
    config.backend = "http";

    const auto payload = decodeSemanticMapPayload(encodeSemanticMapPayload(
        SemanticMapAsyncPayload{.config = config, .inputFields = {"TITLE", "BODY"}, .outputFields = {"SENTIMENT"}}));

    EXPECT_EQ(payload.config.endpoint, config.endpoint);
    EXPECT_EQ(payload.config.modelName, config.modelName);
    EXPECT_EQ(payload.config.datasetPrompt, config.datasetPrompt);
    EXPECT_EQ(payload.config.payloadFormat, PayloadFormat::JSON_OBJECT);
    EXPECT_EQ(payload.config.batchSize, config.batchSize);
    EXPECT_EQ(payload.config.maxConcurrency, config.maxConcurrency);
    EXPECT_EQ(payload.config.maxRetries, config.maxRetries);
    EXPECT_EQ(payload.config.maxWaitTime, config.maxWaitTime);
    EXPECT_EQ(payload.config.requestTimeout, config.requestTimeout);
    EXPECT_EQ(payload.config.backend, config.backend);
    ASSERT_EQ(payload.config.steps.size(), 1U);
    EXPECT_EQ(payload.config.steps.front(), config.steps.front());
    EXPECT_EQ(payload.inputFields, (std::vector<std::string>{"TITLE", "BODY"}));
    EXPECT_EQ(payload.outputFields, (std::vector<std::string>{"SENTIMENT"}));
}

TEST_F(SemanticMapExecutorTest, EncodingCarriesTheVariableNameAndNotTheSecret)
{
    auto config = configWith("echo");
    config.apiKeyEnvVar = "SEMANTIC_MAP_EXECUTOR_TEST_KEY";

    const auto wire = encodeSemanticMapPayload(
        SemanticMapAsyncPayload{.config = config, .inputFields = {"DESCRIPTION"}, .outputFields = {"SENTIMENT"}});

    /// The point of naming the variable rather than storing the key: the encoded form travels to
    /// the worker inside a source descriptor, so a secret in here would travel with it.
    EXPECT_NE(wire.find("SEMANTIC_MAP_EXECUTOR_TEST_KEY"), std::string::npos);
    EXPECT_EQ(decodeSemanticMapPayload(wire).config.apiKeyEnvVar, config.apiKeyEnvVar);
}

TEST_F(SemanticMapExecutorTest, RejectsAGarbledPayload)
{
    EXPECT_THROW((void)decodeSemanticMapPayload("not json"), Exception);
}

TEST_F(SemanticMapExecutorTest, RegistryProvidesTheExecutor)
{
    EXPECT_TRUE(AsyncExecutorRegistry::instance().find("SemanticMap").has_value());
    /// Registry keys are matched case-insensitively, as for sources and sinks.
    EXPECT_TRUE(AsyncExecutorRegistry::instance().find("semanticmap").has_value());
}

TEST_F(SemanticMapExecutorTest, ConfigurationWithoutAModelIsRejected)
{
    const auto factory = AsyncExecutorRegistry::instance().find("SemanticMap");
    ASSERT_TRUE(factory.has_value());
    EXPECT_THROW(
        (*factory)(
            AsyncExecutorRegistryArguments{
                AsyncOperatorContext{.inputLayout = &input, .outputLayout = &output, .config = {}, .batchSize = 1}}),
        Exception);
}

TEST_F(SemanticMapExecutorTest, AnInputFieldMissingFromTheSchemaIsRejected)
{
    const auto factory = AsyncExecutorRegistry::instance().find("SemanticMap");
    ASSERT_TRUE(factory.has_value());
    const auto config = configWith("echo");
    std::unordered_map<std::string, std::string> wrongField{
        {std::string{SemanticMapConfigKey},
         encodeSemanticMapPayload(
             SemanticMapAsyncPayload{.config = config, .inputFields = {"NO_SUCH_FIELD"}, .outputFields = {"SENTIMENT"}})}};
    EXPECT_THROW(
        (*factory)(
            AsyncExecutorRegistryArguments{
                AsyncOperatorContext{
                    .inputLayout = &input, .outputLayout = &output, .config = std::move(wrongField), .batchSize = 1}}),
        Exception);
}

TEST_F(SemanticMapExecutorTest, OneAnswerPerRecordInInputOrder)
{
    const auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto executor = makeExecutor(configWith("echo"));

    const std::vector<std::string> texts{"great documentary", "terrible", "wildly entertaining"};
    const auto records = makeRecords(input, buffer, *bufferManager, texts);

    const auto results = executor->process(records);

    ASSERT_EQ(results.size(), texts.size());
    const auto sentiment = output.indexOf("SENTIMENT");
    ASSERT_TRUE(sentiment.has_value());
    for (const auto& result : results)
    {
        ASSERT_EQ(result.fields.size(), 1U);
        EXPECT_EQ(result.fields.front().fieldIndex, sentiment.value());
    }
    EXPECT_EQ(results[0].fields.front().value, "GREAT DOCUMENTARY");
    EXPECT_EQ(results[1].fields.front().value, "TERRIBLE");
    EXPECT_EQ(results[2].fields.front().value, "WILDLY ENTERTAINING");
}

TEST_F(SemanticMapExecutorTest, AnEmptyBatchAsksNothing)
{
    const auto executor = makeExecutor(configWith("fail"));
    /// 'fail' would throw if a request were made at all.
    EXPECT_TRUE(executor->process({}).empty());
}

TEST_F(SemanticMapExecutorTest, AnUnusableAnswerBecomesTheDefault)
{
    const auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto executor = makeExecutor(configWith("unparseable", {"POSITIVE", "NEGATIVE"}, "n/a"));

    const auto records = makeRecords(input, buffer, *bufferManager, {"great documentary", "terrible"});
    const auto results = executor->process(records);

    ASSERT_EQ(results.size(), 2U);
    EXPECT_EQ(results[0].fields.front().value, "n/a");
    EXPECT_EQ(results[1].fields.front().value, "n/a");
}

TEST_F(SemanticMapExecutorTest, AFailedTransportFailsTheQuery)
{
    const auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto executor = makeExecutor(configWith("fail"));

    const auto records = makeRecords(input, buffer, *bufferManager, {"great documentary"});
    /// Unlike an unusable answer, an unreachable endpoint is the deployment's problem, and the
    /// synchronous operator fails the query over it too. The two modes must agree here.
    EXPECT_THROW(executor->process(records), Exception);
}

TEST_F(SemanticMapExecutorTest, EveryCallingThreadGetsItsOwnBackend)
{
    const auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto executor = makeExecutor(configWith("echo"));

    const std::vector<std::string> texts{"one", "two", "three", "four"};
    const auto records = makeRecords(input, buffer, *bufferManager, texts);

    /// A backend owns a curl handle and is not thread-safe, while the framework calls process on
    /// as many threads as maxConcurrency allows.
    constexpr size_t Threads = 4;
    std::vector<std::thread> callers;
    std::vector<std::vector<AsyncRecordResult>> perThread(Threads);
    callers.reserve(Threads);
    for (size_t thread = 0; thread < Threads; ++thread)
    {
        callers.emplace_back([&, thread] { perThread[thread] = executor->process(records); });
    }
    for (auto& caller : callers)
    {
        caller.join();
    }

    for (const auto& results : perThread)
    {
        ASSERT_EQ(results.size(), texts.size());
        EXPECT_EQ(results[0].fields.front().value, "ONE");
        EXPECT_EQ(results[3].fields.front().value, "FOUR");
    }
}

}
