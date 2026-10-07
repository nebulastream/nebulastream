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
#include <unordered_map>
#include <utility>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>
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
#include <AsyncExecutorRegistry.hpp>
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

SemanticStep filterStep()
{
    return SemanticStep{
        .kind = SemanticStep::Kind::FILTER, .prompt = "The review is positive", .outputColumn = {}, .outputValues = {}, .defaultValue = {}};
}

/// The mock backend reads the endpoint as a behaviour: 'echo' answers with the row's text
/// upper-cased — a verdict passes for "true" or "yes" — 'label:<X>' with X for every row,
/// 'unparseable' with prose, 'fail' not at all.
SemanticModelConfig configWith(const std::string& behaviour, std::vector<SemanticStep> steps = {filterStep()})
{
    return SemanticModelConfig{
        .endpoint = behaviour,
        .modelName = "mock-model",
        .datasetPrompt = {},
        .steps = std::move(steps),
        .payloadFormat = PayloadFormat::SPACE_JOINED,
        .batchSize = 4,
        .maxConcurrency = 4,
        .maxRetries = 0,
        .maxWaitTime = std::chrono::milliseconds{1000},
        .requestTimeout = std::chrono::seconds{600},
        .apiKeyEnvVar = std::nullopt,
        .backend = "mock",
        .execution = SemanticExecution::ASYNCHRONOUS,
        .preserveOrder = true,
        .fusion = true};
}

std::vector<bool> keptOf(const std::vector<AsyncRecordResult>& results)
{
    std::vector<bool> kept;
    for (const auto& result : results)
    {
        kept.push_back(result.keep);
    }
    return kept;
}
}

class SemanticFilterExecutorTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SemanticFilterExecutorTest.log", LogLevel::LOG_DEBUG); }

    AsyncRecordLayout input{Schema<UnqualifiedUnboundField, Ordered>{field("description", DataType::Type::VARSIZED)}};
    /// What a plain filter produces: its input, unchanged.
    AsyncRecordLayout passthrough{Schema<UnqualifiedUnboundField, Ordered>{field("description", DataType::Type::VARSIZED)}};
    /// What a filter fused with a map produces: the map's column appended.
    AsyncRecordLayout fused{Schema<UnqualifiedUnboundField, Ordered>{
        field("description", DataType::Type::VARSIZED), field("sentiment", DataType::Type::VARSIZED)}};

    std::unique_ptr<AsyncOperatorExecutor>
    makeExecutor(const SemanticModelConfig& config, const AsyncRecordLayout& output, std::vector<std::string> outputFields = {}) const
    {
        const auto factory = AsyncExecutorRegistry::instance().find("SemanticFilter");
        INVARIANT(factory.has_value(), "SemanticFilter executor is not registered");
        std::unordered_map<std::string, std::string> encoded{
            {std::string{SemanticMapConfigKey},
             encodeSemanticMapPayload(
                 SemanticMapAsyncPayload{.config = config, .inputFields = {"DESCRIPTION"}, .outputFields = std::move(outputFields)})}};
        return (*factory)(AsyncExecutorRegistryArguments{AsyncOperatorContext{
            .inputLayout = &input, .outputLayout = &output, .config = std::move(encoded), .batchSize = config.batchSize}});
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

TEST_F(SemanticFilterExecutorTest, RegistryProvidesTheExecutor)
{
    EXPECT_TRUE(AsyncExecutorRegistry::instance().find("SemanticFilter").has_value());
    EXPECT_TRUE(AsyncExecutorRegistry::instance().find("semanticfilter").has_value());
}

TEST_F(SemanticFilterExecutorTest, PayloadCarriesFilterStepsAndFusion)
{
    const auto config = configWith("echo");
    const auto payload = decodeSemanticMapPayload(
        encodeSemanticMapPayload(SemanticMapAsyncPayload{.config = config, .inputFields = {"DESCRIPTION"}, .outputFields = {}}));
    ASSERT_EQ(payload.config.steps.size(), 1U);
    EXPECT_EQ(payload.config.steps.front(), filterStep());
    EXPECT_TRUE(payload.config.fusion);
    EXPECT_TRUE(payload.outputFields.empty());
}

TEST_F(SemanticFilterExecutorTest, KeepsTheAffirmedRecordsOnly)
{
    const auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto executor = makeExecutor(configWith("echo"), passthrough);

    const auto records = makeRecords(input, buffer, *bufferManager, {"true", "no", "Yes", "false", "maybe"});
    const auto results = executor->process(records);

    EXPECT_EQ(keptOf(results), (std::vector{true, false, true, false, false}));
    for (const auto& result : results)
    {
        /// A plain filter adds no field; the framework copies the record over as it is.
        EXPECT_TRUE(result.fields.empty());
    }
}

TEST_F(SemanticFilterExecutorTest, ConstantVerdicts)
{
    const auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto records = makeRecords(input, buffer, *bufferManager, {"a", "b", "c"});

    EXPECT_EQ(keptOf(makeExecutor(configWith("label:true"), passthrough)->process(records)), (std::vector{true, true, true}));
    EXPECT_EQ(keptOf(makeExecutor(configWith("label:false"), passthrough)->process(records)), (std::vector{false, false, false}));
}

TEST_F(SemanticFilterExecutorTest, AnUnusableAnswerDropsEveryRecord)
{
    const auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto executor = makeExecutor(configWith("unparseable"), passthrough);

    const auto records = makeRecords(input, buffer, *bufferManager, {"true", "yes"});
    EXPECT_EQ(keptOf(executor->process(records)), (std::vector{false, false}));
}

TEST_F(SemanticFilterExecutorTest, AFailedTransportFailsTheQuery)
{
    const auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto executor = makeExecutor(configWith("fail"), passthrough);

    const auto records = makeRecords(input, buffer, *bufferManager, {"true"});
    EXPECT_THROW(executor->process(records), Exception);
}

TEST_F(SemanticFilterExecutorTest, AnEmptyBatchAsksNothing)
{
    const auto executor = makeExecutor(configWith("fail"), passthrough);
    EXPECT_TRUE(executor->process({}).empty());
}

/// A map fused into the filter: kept records get the map's answer, dropped ones nothing.
TEST_F(SemanticFilterExecutorTest, FusedMapStepsFillTheKeptRecords)
{
    const auto bufferManager = makeBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    const auto mapStep = SemanticStep{
        .kind = SemanticStep::Kind::MAP,
        .prompt = "Classify the sentiment",
        .outputColumn = "SENTIMENT",
        .outputValues = {},
        .defaultValue = {}};
    const auto executor = makeExecutor(configWith("echo", {mapStep, filterStep()}), fused, {"SENTIMENT"});

    const auto records = makeRecords(input, buffer, *bufferManager, {"yes", "no"});
    const auto results = executor->process(records);

    ASSERT_EQ(results.size(), 2U);
    EXPECT_TRUE(results[0].keep);
    ASSERT_EQ(results[0].fields.size(), 1U);
    EXPECT_EQ(results[0].fields.front().fieldIndex, fused.indexOf("SENTIMENT").value());
    EXPECT_EQ(results[0].fields.front().value, "YES");
    EXPECT_FALSE(results[1].keep);
}

TEST_F(SemanticFilterExecutorTest, OutputFieldsMustMatchTheMapSteps)
{
    /// A plain filter declares no OUTPUT field, so one in the payload means a broken plan.
    EXPECT_THROW((void)makeExecutor(configWith("echo"), fused, {"SENTIMENT"}), Exception);
}

}
